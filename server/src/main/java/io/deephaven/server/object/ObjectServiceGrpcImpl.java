/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.object;

import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.session.TicketRouter;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.lang.Object;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
                    ObjectType objectTypeInstance = getObjectTypeInstance(type, o);

                    StreamObserver<StreamResponse> wrappedResponseObserver = new StreamObserver<>() {
                        @Override
                        public void onNext(StreamResponse value) {
                            // TODO Do we send a message here, _then_ error if the plugin didn't close? Or do we close
                            // with an error without sending anything (and release exports)?
                            responseObserver.onNext(FetchObjectResponse.newBuilder()
                                    .setType(type)
                                    .setData(value.getData().getPayload())
                                    .addAllTypedExportIds(value.getData().getTypedExportIdsList())
                                    .build());
                        }

                        @Override
                        public void onError(Throwable t) {
                            responseObserver.onError(t);
                        }

                        @Override
                        public void onCompleted() {
                            responseObserver.onCompleted();
                        }
                    };
                    PluginMessageSender connection = new PluginMessageSender(wrappedResponseObserver, session);
                    objectTypeInstance.clientConnection(o, connection);

                    if (!connection.isClosed()) {
                        connection.close();
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "Plugin didn't close response, use MessageStream instead for this object");
                    }

                    return null;
                });
    }

    @Override
    public StreamObserver<StreamRequest> messageStream(
            StreamObserver<StreamResponse> responseObserver) {
        return new SendMessageObserver(responseObserver);
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

    private void cleanup(Collection<ExportObject<?>> exports, Throwable t) {
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

        private boolean closed;

        public PluginMessageSender(StreamObserver<StreamResponse> responseObserver, SessionState sessionState) {
            this.responseObserver = responseObserver;
            this.sessionState = sessionState;
        }

        @Override
        public void onMessage(ByteBuffer message, Object[] references) {

            Data.Builder payload = Data.newBuilder().setPayload(ByteString.copyFrom(message));

            List<ExportObject<?>> exports = new ArrayList<>();
            try {
                for (Object reference : references) {
                    final String type = typeLookup.type(reference).orElse(null);
                    final ExportObject<?> exportObject = sessionState.newServerSideExport(reference);
                    exports.add(exportObject);
                    TypedTicket typedTicket = ticketForExport(exportObject, type);
                    payload.addTypedExportIds(typedTicket);
                }
            } catch (RuntimeException | Error t) {
                cleanup(exports, t);
                throw t;
            } catch (Throwable t) {
                cleanup(exports, t);
                throw new RuntimeException(t);
            }
            final StreamResponse.Builder responseBuilder =
                    StreamResponse.newBuilder().setData(payload);
            GrpcUtil.safelyOnNext(responseObserver, responseBuilder.build());
        }

        private TypedTicket ticketForExport(ExportObject<?> exportObject, String type) {
            TypedTicket.Builder builder = TypedTicket.newBuilder().setTicket(exportObject.getExportId());
            if (type != null) {
                builder.setType(type);
            }
            return builder.build();
        }

        @Override
        public void close() {
            closed = true;
            responseObserver.onCompleted();
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
