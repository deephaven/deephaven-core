//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.object;

import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.function.ThrowingRunnable;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.lang.Object;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ObjectServiceGrpcImpl extends ObjectServiceGrpc.ObjectServiceImplBase {
    private final SessionService sessionService;
    private final TicketRouter ticketRouter;
    private final ObjectTypeLookup objectTypeLookup;
    private final TypeLookup typeLookup;
    private final SessionService.ErrorTransformer errorTransformer;

    @FunctionalInterface
    interface StreamOperation extends ThrowingRunnable<ObjectCommunicationException> {
    }

    @Inject
    public ObjectServiceGrpcImpl(SessionService sessionService, TicketRouter ticketRouter,
            ObjectTypeLookup objectTypeLookup, TypeLookup typeLookup,
            SessionService.ErrorTransformer errorTransformer) {
        this.sessionService = Objects.requireNonNull(sessionService);
        this.ticketRouter = Objects.requireNonNull(ticketRouter);
        this.objectTypeLookup = Objects.requireNonNull(objectTypeLookup);
        this.typeLookup = Objects.requireNonNull(typeLookup);
        this.errorTransformer = Objects.requireNonNull(errorTransformer);
    }

    private enum EnqueuedState {
        WAITING, RUNNING, CLOSED
    }
    private final class SendMessageObserver implements StreamObserver<StreamRequest> {
        private final SessionState session;
        private final StreamObserver<StreamResponse> responseObserver;

        private boolean seenConnect = false;
        private ObjectType.MessageStream messageStream;

        private final Queue<EnqueuedStreamOperation> operations = new ConcurrentLinkedQueue<>();
        private final AtomicReference<EnqueuedState> runState = new AtomicReference<>(EnqueuedState.WAITING);

        class EnqueuedStreamOperation {
            private final StreamOperation wrapped;
            private final SessionState.ExportBuilder<Object> nonExport;

            EnqueuedStreamOperation(Collection<? extends ExportObject<?>> dependencies,
                    StreamOperation wrapped) {
                this.wrapped = wrapped;
                this.nonExport = session.nonExport()
                        .onErrorHandler(SendMessageObserver.this::onError)
                        .require(List.copyOf(dependencies));
            }

            public void run() {
                nonExport.submit(() -> {
                    if (runState.get() == EnqueuedState.CLOSED) {
                        return;
                    }
                    // Run the specified work. Note that we're not concerned about exceptions, the stream will
                    // be dead (via onError) and won't be used again.
                    try {
                        wrapped.run();
                    } catch (ObjectCommunicationException e) {
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "Error performing MessageStream operation");
                    }

                    // Set state to WAITING if it is RUNNING so that any new work can race being added
                    if (runState.compareAndSet(EnqueuedState.RUNNING, EnqueuedState.WAITING)) {
                        doWork();
                    } // else the stream should be ended and no more work done
                });
            }
        }

        private SendMessageObserver(SessionState session, StreamObserver<StreamResponse> responseObserver) {
            this.session = session;
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(final StreamRequest request) {
            GrpcErrorHelper.checkHasOneOf(request, "message");
            if (request.hasConnect()) {
                if (seenConnect) {
                    throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                            "Already sent a connect request, cannot send another");
                }
                seenConnect = true;

                TypedTicket typedTicket = request.getConnect().getSourceId();

                final String type = typedTicket.getType();
                if (type.isEmpty()) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "No type supplied");
                }
                if (typedTicket.getTicket().getTicket().isEmpty()) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "No ticket supplied");
                }

                final SessionState.ExportObject<Object> object = ticketRouter.resolve(
                        session, typedTicket.getTicket(), "sourceId");

                runOrEnqueue(Collections.singleton(object), () -> {
                    final Object o = object.get();
                    final ObjectType objectType = getObjectTypeInstance(type, o);

                    PluginMessageSender clientConnection = new PluginMessageSender(responseObserver, session);
                    messageStream = objectType.clientConnection(o, clientConnection);
                });
            } else if (request.hasData()) {
                if (!seenConnect) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Data message sent before Connect message");
                }
                ClientData data = request.getData();
                LivenessScope exportScope = new LivenessScope();

                List<SessionState.ExportObject<Object>> referenceObjects;
                try (SafeCloseable ignored = LivenessScopeStack.open(exportScope, false)) {
                    referenceObjects = data.getReferencesList().stream()
                            .map(typedTicket -> ticketRouter.resolve(session, typedTicket.getTicket(), "ticket"))
                            .collect(Collectors.toList());
                }
                runOrEnqueue(referenceObjects, () -> {
                    Object[] objs;
                    try {
                        objs = referenceObjects.stream().map(ExportObject::get).toArray();
                    } finally {
                        exportScope.release();
                    }
                    messageStream.onData(data.getPayload().asReadOnlyByteBuffer(), objs);
                });
            }
        }

        /**
         * Helper to serialize incoming ObjectType messages. These methods are intended to roughly behave like
         * SerializingExecutor(directExecutor()) in that only one can be running at a time, and will be started on the
         * current thread, with the distinction that submitted work will continue off-thread and will signal when it is
         * finished.
         *
         * @param dependencies other ExportObjects that must be resolve to perform the operation
         * @param operation the lambda to execute when it is our turn to run
         */
        private void runOrEnqueue(Collection<? extends ExportObject<?>> dependencies, StreamOperation operation) {
            // gRPC guarantees we can't race enqueuing
            operations.add(new EnqueuedStreamOperation(dependencies, operation));
            doWork();
        }

        private void doWork() {
            // More than one thread (at most two) can arrive here at the same time, but only one will pass the
            // compareAndSet
            EnqueuedStreamOperation next = operations.peek();

            // If we fail the null check, no work to do, leave state as WAITING (though if work was added right after
            // peek(), that thread will make it into here and start). If we fail the state check, something else has
            // already started work
            if (next != null && runState.compareAndSet(EnqueuedState.WAITING, EnqueuedState.RUNNING)) {
                // We successfully set state to running, and should remove the item we just peeked at
                EnqueuedStreamOperation actualNext = operations.poll();
                Assert.eq(next, "next", actualNext, "actualNext");

                // Run the new item
                next.run();
            }
        }

        @Override
        public void onError(final Throwable t) {
            // Avoid starting more work
            runState.set(EnqueuedState.CLOSED);

            // Safely inform the client that an error happened
            GrpcUtil.safelyError(responseObserver, errorTransformer.transform(t));

            // If the objecttype connection was opened, close it and refuse later calls from it
            if (messageStream != null) {
                closeMessageStream();
            }

            // Don't attempt to run additional work
            operations.clear();
        }

        private void closeMessageStream() {
            try {
                messageStream.onClose();
            } catch (Exception ignored) {
                // ignore errors from closing the plugin
            }
        }

        @Override
        public void onCompleted() {
            // Don't finalize until we've processed earlier messages
            runOrEnqueue(Collections.emptyList(), () -> {
                runState.set(EnqueuedState.CLOSED);
                // Respond by closing the stream - note that closing here allows the server plugin to respond to earlier
                // messages without error, but those responses will be ignored by the client.
                GrpcUtil.safelyComplete(responseObserver);

                // Let the server plugin know that the remote end has closed
                if (messageStream != null) {
                    closeMessageStream();
                }
            });
        }
    }

    @Override
    public void fetchObject(
            @NotNull final FetchObjectRequest request,
            @NotNull final StreamObserver<FetchObjectResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();
        final String type = request.getSourceId().getType();
        if (type.isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "No type supplied");
        }
        if (request.getSourceId().getTicket().getTicket().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "No ticket supplied");
        }

        final String description = "ObjectService#fetchObject(object="
                + ticketRouter.getLogNameFor(request.getSourceId().getTicket(), "sourceId") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<Object> object =
                    ticketRouter.resolve(session, request.getSourceId().getTicket(), "sourceId");

            session.nonExport()
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .require(object)
                    .onError(responseObserver)
                    .submit(() -> {
                        final Object o = object.get();
                        ObjectType objectTypeInstance = getObjectTypeInstance(type, o);

                        AtomicReference<FetchObjectResponse> singleResponse = new AtomicReference<>();
                        AtomicBoolean isClosed = new AtomicBoolean(false);
                        StreamObserver<StreamResponse> wrappedResponseObserver = new StreamObserver<>() {
                            @Override
                            public void onNext(StreamResponse value) {
                                singleResponse.set(FetchObjectResponse.newBuilder()
                                        .setType(type)
                                        .setData(value.getData().getPayload())
                                        .addAllTypedExportIds(value.getData().getExportedReferencesList())
                                        .build());
                            }

                            @Override
                            public void onError(Throwable t) {
                                responseObserver.onError(t);
                            }

                            @Override
                            public void onCompleted() {
                                isClosed.set(true);
                            }
                        };
                        PluginMessageSender connection = new PluginMessageSender(wrappedResponseObserver, session);
                        objectTypeInstance.clientConnection(o, connection);

                        FetchObjectResponse message = singleResponse.get();
                        if (message == null) {
                            connection.onClose();
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Plugin didn't send a response before returning from clientConnection()");
                        }
                        if (!isClosed.get()) {
                            connection.onClose();
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Plugin didn't close response, use MessageStream instead for this object");
                        }
                        GrpcUtil.safelyComplete(responseObserver, message);

                        return null;
                    });
        }
    }

    @Override
    public StreamObserver<StreamRequest> messageStream(StreamObserver<StreamResponse> responseObserver) {
        SessionState session = sessionService.getCurrentSession();
        // Session close logic implicitly handled in
        // io.deephaven.server.session.SessionServiceGrpcImpl.SessionServiceInterceptor
        return new SendMessageObserver(session, responseObserver);
    }

    @NotNull
    private ObjectType getObjectTypeInstance(String expectedType, Object object) {
        // TODO(deephaven-core#1872): Optimize ObjectTypeLookup
        final Optional<ObjectType> o = objectTypeLookup.findObjectType(object);
        if (o.isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.NOT_FOUND,
                    String.format("No ObjectType found, expected type '%s'", expectedType));
        }
        final ObjectType objectType = o.get();
        if (!expectedType.equals(objectType.name())) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, String.format(
                    "Unexpected ObjectType, expected type '%s', actual type '%s'", expectedType, objectType.name()));
        }
        return objectType;
    }

    private static void cleanup(Collection<ExportObject<?>> exports, Throwable t) {
        for (ExportObject<?> export : exports) {
            try {
                export.release();
            } catch (Throwable inner) {
                t.addSuppressed(inner);
            }
        }
    }

    private final class PluginMessageSender implements ObjectType.MessageStream {
        private final StreamObserver<StreamResponse> responseObserver;
        private final SessionState sessionState;

        public PluginMessageSender(StreamObserver<StreamResponse> responseObserver, SessionState sessionState) {
            this.responseObserver = responseObserver;
            this.sessionState = sessionState;
        }

        @Override
        public void onData(ByteBuffer message, Object[] references) throws ObjectCommunicationException {
            List<ExportObject<?>> exports = new ArrayList<>(references.length);
            try {
                ServerData.Builder payload = ServerData.newBuilder().setPayload(ByteString.copyFrom(message));

                for (Object reference : references) {
                    final String type = typeLookup.type(reference).orElse(null);
                    final ExportObject<?> exportObject = sessionState.newServerSideExport(reference);
                    exports.add(exportObject);
                    TypedTicket typedTicket = ticketForExport(exportObject, type);
                    payload.addExportedReferences(typedTicket);
                }
                final StreamResponse.Builder responseBuilder =
                        StreamResponse.newBuilder().setData(payload);

                // Explicitly running this unsafely, we want the exception to clean up, but we still need to synchronize
                // as would normally be done in safelyOnNext
                StreamResponse response = responseBuilder.build();
                synchronized (responseObserver) {
                    responseObserver.onNext(response);
                }
            } catch (Throwable t) {
                // Release any exports we failed to send, and report this as a checked exception
                cleanup(exports, t);
                throw new ObjectCommunicationException(t);
            }
        }

        private TypedTicket ticketForExport(ExportObject<?> exportObject, String type) {
            TypedTicket.Builder builder = TypedTicket.newBuilder().setTicket(exportObject.getExportId());
            if (type != null) {
                builder.setType(type);
            }
            return builder.build();
        }

        @Override
        public void onClose() {
            GrpcUtil.safelyComplete(responseObserver);
        }
    }
}
