/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.object;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil.ExposedByteArrayOutputStream;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectType.Exporter;
import io.deephaven.plugin.type.ObjectType.Exporter.Reference;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse.Builder;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.session.TicketRouter;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.io.IOException;
import java.lang.Object;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public class ObjectServiceGrpcImpl extends ObjectServiceGrpc.ObjectServiceImplBase {
    private final SessionService sessionService;
    private final TicketRouter ticketRouter;
    private final ObjectTypeLookup objectTypeLookup;
    private final TypeLookup typeLookup;

    @Inject
    public ObjectServiceGrpcImpl(SessionService sessionService, TicketRouter ticketRouter,
            ObjectTypeLookup objectTypeLookup, TypeLookup typeLookup) {
        this.sessionService = Objects.requireNonNull(sessionService);
        this.ticketRouter = Objects.requireNonNull(ticketRouter);
        this.objectTypeLookup = Objects.requireNonNull(objectTypeLookup);
        this.typeLookup = Objects.requireNonNull(typeLookup);
    }

    private final class SendMessageObserver extends SingletonLivenessManager implements StreamObserver<StreamRequest> {

        private ExportObject<Object> object;
        private final StreamObserver<StreamResponse> responseObserver;

        private ObjectType.MessageStream messageStream;

        private SendMessageObserver(StreamObserver<StreamResponse> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(final StreamRequest request) {
            SessionState session = sessionService.getCurrentSession();

            if (request.hasConnect()) {
                // Should only appear in the first request
                if (this.object != null) {
                    throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                            "Already sent a connect request, cannot send another");
                }

                TypedTicket typedTicket = request.getConnect().getTypedTicket();

                final String type = typedTicket.getType();
                if (type.isEmpty()) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "No type supplied");
                }
                if (typedTicket.getTicket().getTicket().isEmpty()) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "No ticket supplied");
                }

                final SessionState.ExportObject<Object> object = ticketRouter.resolve(
                        session, typedTicket.getTicket(), "sourceId");

                this.object = object;
                manage(this.object);
                session.nonExport().require(object).onError(responseObserver).submit(() -> {
                    final Object o = object.get();
                    final ObjectType objectType = getObjectTypeInstance(type, o);

                    messageStream = objectType.clientConnection(o, new PluginMessageSender(responseObserver, session));
                });
            } else if (request.hasData()) {
                // All other requests
                Data data = request.getData();
                List<SessionState.ExportObject<Object>> referenceObjects = data.getTypedExportIdsList().stream()
                        .map(typedTicket -> ticketRouter.resolve(session, typedTicket.getTicket(), "messageObjectId"))
                        .collect(Collectors.toList());
                List<SessionState.ExportObject<Object>> requireObjects = new ArrayList<>(referenceObjects);
                requireObjects.add(object);
                session.nonExport().require(requireObjects).onError(responseObserver).submit(() -> {
                    Object[] objs = referenceObjects.stream().map(ExportObject::get).toArray();
                    // TODO: Should we try/catch this to recover/not close from plugin errors? Or make the client
                    // recover? Would be useful if the plugin is complex
                    messageStream.onMessage(data.getPayload().asReadOnlyByteBuffer(), objs);
                });
            } else {
                // Do something with unexpected message type?
            }
        }

        @Override
        public void onError(final Throwable t) {
            // Do we need to clean up export collector here and onCompleted too?
            release();
        }

        @Override
        public void onCompleted() {
            SessionState session = sessionService.getCurrentSession();
            session.nonExport().require(object).onError(responseObserver).submit(() -> {
                messageStream.close();
                responseObserver.onCompleted();
            });
            release();
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
        final SessionState.ExportObject<Object> object = ticketRouter.resolve(
                session, request.getSourceId().getTicket(), "sourceId");
        session.nonExport()
                .require(object)
                .onError(responseObserver)
                .submit(() -> {
                    final Object o = object.get();
                    final ExportCollector exportCollector = new ExportCollector(session);
                    ObjectType objectTypeInstance = getObjectTypeInstance(type, o);
                    final Builder builder = FetchObjectResponse.newBuilder()
                            .setType(type)
                            .setData(serialize(objectTypeInstance, o, exportCollector))
                            .addAllTypedExportIds(exportCollector.refs().stream().map(ReferenceImpl::typedTicket)
                                    .collect(Collectors.toList()));

                    GrpcUtil.safelyComplete(responseObserver, builder.build());
                    return null;
                });
    }

    @Override
    public StreamObserver<StreamRequest> messageStream(
            StreamObserver<StreamResponse> responseObserver) {
        return new SendMessageObserver(responseObserver);
    }

    private ByteString serialize(final ObjectType objectType, Object object, ExportCollector exportCollector)
            throws IOException {
        final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream();
        try {
            objectType.writeTo(exportCollector, object, out);
            return ByteStringAccess.wrap(out.peekBuffer(), 0, out.size());
        } catch (Throwable t) {
            exportCollector.cleanup(t);
            throw t;
        }
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

    private static boolean referenceEquality(Object t, Object u) {
        return t == u;
    }

    final class ExportCollector {

        private final SessionState sessionState;
        private final Thread thread;
        private final List<ReferenceImpl> references;

        public ExportCollector(SessionState sessionState) {
            this.sessionState = Objects.requireNonNull(sessionState);
            this.thread = Thread.currentThread();
            this.references = new ArrayList<>();
        }

        public List<ReferenceImpl> refs() {
            return references;
        }

        public Reference reference(Object object) {
            // noinspection OptionalGetWithoutIsPresent
            return reference(object, true, true).get();
        }

        public Optional<Reference> reference(Object object, boolean allowUnknownType, boolean forceNew) {
            return reference(object, allowUnknownType, forceNew, ObjectServiceGrpcImpl::referenceEquality);
        }

        public Optional<Reference> reference(Object object, boolean allowUnknownType, boolean forceNew,
                BiPredicate<Object, Object> equals) {
            if (thread != Thread.currentThread()) {
                throw new IllegalStateException("Should only create references on the calling thread");
            }
            if (!forceNew) {
                for (ReferenceImpl reference : references) {
                    if (equals.test(object, reference.export.get())) {
                        return Optional.of(reference);
                    }
                }
            }
            return newReferenceImpl(object, allowUnknownType);
        }

        private Optional<Reference> newReferenceImpl(Object object, boolean allowUnknownType) {
            final String type = typeLookup.type(object).orElse(null);
            if (!allowUnknownType && type == null) {
                return Optional.empty();
            }
            final ExportObject<?> exportObject = sessionState.newServerSideExport(object);
            final ReferenceImpl ref = new ReferenceImpl(references.size(), type, exportObject);
            references.add(ref);
            return Optional.of(ref);
        }

        private void cleanup(Throwable t) {
            for (ReferenceImpl ref : refs()) {
                try {
                    ref.export.release();
                } catch (Throwable inner) {
                    t.addSuppressed(inner);
                }
            }
        }
    }

    private static final class ReferenceImpl implements Reference {
        private final int index;
        private final String type;
        private final ExportObject<?> export;

        public ReferenceImpl(int index, String type, ExportObject<?> export) {
            this.index = index;
            this.type = type;
            this.export = Objects.requireNonNull(export);
        }

        public TypedTicket typedTicket() {
            final TypedTicket.Builder builder = TypedTicket.newBuilder().setTicket(export.getExportId());
            if (type != null) {
                builder.setType(type);
            }
            return builder.build();
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public Optional<String> type() {
            return Optional.ofNullable(type);
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
        public void onMessage(ByteBuffer message, Object[] references) {
            ExportCollector exportCollector = new ExportCollector(sessionState);

            Data.Builder payload = Data.newBuilder().setPayload(ByteString.copyFrom(message));

            try {
                for (Object reference : references) {
                    // allowUnknownType=true so that the caller can let the client handle complex lifecycles
                    // forceNew=true to explicitly state that the plugin is responsible for handling dups, both
                    // within a single payload and across the stream
                    // noinspection OptionalGetWithoutIsPresent
                    exportCollector.reference(reference, true, true).get();
                }
                for (ReferenceImpl ref : exportCollector.refs()) {
                    payload.addTypedExportIds(ref.typedTicket());
                }
            } catch (RuntimeException | Error t) {
                exportCollector.cleanup(t);
                throw t;
            } catch (Throwable t) {
                exportCollector.cleanup(t);
                throw new RuntimeException(t);
            }
            final StreamResponse.Builder responseBuilder =
                    StreamResponse.newBuilder().setData(payload);
            GrpcUtil.safelyOnNext(responseObserver, responseBuilder.build());
        }

        @Override
        public void close() {
            responseObserver.onCompleted();
        }
    }
}
