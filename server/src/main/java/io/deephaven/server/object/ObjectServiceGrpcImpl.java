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
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectType.Exporter;
import io.deephaven.plugin.type.ObjectType.Exporter.Reference;
import io.deephaven.plugin.type.ObjectType.MessageSender;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class ObjectServiceGrpcImpl extends ObjectServiceGrpc.ObjectServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ObjectServiceGrpcImpl.class);

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

    private final class SendMessageObserver extends SingletonLivenessManager implements StreamObserver<MessageRequest> {

        private ExportObject<Object> object;
        private final StreamObserver<MessageResponse> responseObserver;

        private SendMessageObserver(StreamObserver<MessageResponse> responseObserver) {
            log.info().append("Creating observer").endl();
            this.responseObserver = responseObserver;
        }

        private ObjectType getObjectType() {
            final Optional<ObjectType> o = objectTypeLookup.findObjectType(object.get());
            if (o.isEmpty()) {
                throw Exceptions.statusRuntimeException(Code.NOT_FOUND,
                        String.format("No ObjectType found for object %s", object.get()));
            }
            return o.get();
        }

        @Override
        public void onNext(final MessageRequest request) {
            SessionState session = sessionService.getCurrentSession();

            if (request.hasSourceId()) {
                // First request
                if (this.object != null) {
                    // Ignore duplicate connection requests on the same stream
                    return;
                }

                TypedTicket typedTicket = request.getSourceId().getTypedTicket();

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
                    PluginMessageSender sender = new PluginMessageSender(responseObserver, session);
                    final Object o = object.get();
                    final ObjectType objectType = getObjectType();
                    final ExportCollector exportCollector = sender.getExporter();
                    final MessageResponse.Builder builder = MessageResponse.newBuilder();

                    try {
                        builder.setData(serialize(type, o, exportCollector))
                                .addAllTypedExportId(exportCollector.refs().stream().map(ReferenceImpl::typedTicket)
                                        .collect(Collectors.toList()));
                    } catch (Throwable t) {
                        exportCollector.cleanup(t);
                        throw new RuntimeException(t);
                    }
                    responseObserver.onNext(builder.build());
                    if (objectType.supportsBidiMessaging(o)) {
                        objectType.addMessageSender(o, sender);
                    } else {
                        responseObserver.onCompleted();
                        onCompleted();
                    }
                });
            } else if (request.hasData()) {
                // All other requests
                DataRequest data = request.getData();
                List<SessionState.ExportObject<Object>> referenceObjects = data.getTypedExportIdList().stream()
                        .map(typedTicket -> ticketRouter.resolve(session, typedTicket.getTicket(), "messageObjectId"))
                        .collect(Collectors.toList());
                List<SessionState.ExportObject<Object>> requireObjects = new ArrayList<>(referenceObjects);
                requireObjects.add(object);
                session.nonExport().require(requireObjects).onError(responseObserver).submit(() -> {
                    byte[] msg = data.getData().toByteArray();
                    Object[] objs = referenceObjects.stream().map(ExportObject::get).toArray();
                    // TODO: Should we try/catch this to recover/not close from plugin errors? Or make the client
                    // recover? Would be useful if the plugin is complex
                    getObjectType().handleMessage(msg, object.get(), objs);
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
                getObjectType().removeMessageSender();
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
                    final Builder builder = FetchObjectResponse.newBuilder()
                            .setType(type)
                            .setData(serialize(type, o, exportCollector))
                            .addAllTypedExportId(exportCollector.refs().stream().map(ReferenceImpl::typedTicket)
                                    .collect(Collectors.toList()));

                    GrpcUtil.safelyComplete(responseObserver, builder.build());
                    return null;
                });
    }

    @Override
    public StreamObserver<MessageRequest> messageStream(
            StreamObserver<MessageResponse> responseObserver) {
        return new SendMessageObserver(responseObserver);
    }

    private ByteString serialize(String expectedType, Object object, ExportCollector exportCollector)
            throws IOException {
        final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream();
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
        try {
            objectType.writeTo(exportCollector, object, out);
            return ByteStringAccess.wrap(out.peekBuffer(), 0, out.size());
        } catch (Throwable t) {
            exportCollector.cleanup(t);
            throw t;
        }
    }

    class ExportCollector implements Exporter {

        private final SessionState sessionState;
        private final Thread thread;

        private final HashMap<Object, ReferenceImpl> refMap;

        private final ArrayList<ReferenceImpl> refQueue;

        public ExportCollector(SessionState sessionState) {
            this.sessionState = Objects.requireNonNull(sessionState);
            this.thread = Thread.currentThread();
            this.refMap = new LinkedHashMap<>();
            this.refQueue = new ArrayList<>();
        }

        public List<ReferenceImpl> refs() {
            final ArrayList<ReferenceImpl> refs = new ArrayList<>(refQueue);
            refQueue.clear();
            return refs;
        }

        @Override
        public synchronized Optional<Reference> reference(Object object, boolean allowUnknownType, boolean forceNew) {
            ReferenceImpl oldRef = refMap.get(object);

            if (oldRef != null && !forceNew) {
                return Optional.of(oldRef);
            }

            ReferenceImpl newRef = newReferenceImpl(object, allowUnknownType, refMap.size());
            refMap.put(object, newRef);
            refQueue.add(newRef);

            return newRef == null ? Optional.empty() : Optional.of(newRef);
        }

        private ReferenceImpl newReferenceImpl(Object object, boolean allowUnknownType, int index) {
            final String type = typeLookup.type(object).orElse(null);
            if (!allowUnknownType && type == null) {
                return null;
            }
            final ExportObject<?> exportObject = sessionState.newServerSideExport(object);
            return new ReferenceImpl(index, type, exportObject);
        }

        public void cleanup(Throwable t) {
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

    private final class PluginMessageSender extends ExportCollector implements MessageSender {

        private final StreamObserver<MessageResponse> responseObserver;

        private final ExportCollector exportCollector;

        public PluginMessageSender(StreamObserver<MessageResponse> responseObserver, SessionState sessionState) {
            super(sessionState);
            this.responseObserver = responseObserver;
            exportCollector = new ExportCollector(sessionState);
        }

        public ExportCollector getExporter() {
            return exportCollector;
        }

        @Override
        public void sendMessage(byte[] msg) {
            final MessageResponse.Builder responseBuilder =
                    MessageResponse.newBuilder().setData(ByteString.copyFrom(msg));
            try {
                for (ReferenceImpl ref : exportCollector.refs()) {
                    if (ref != null) {
                        responseBuilder.addTypedExportId(ref.typedTicket());
                    }
                }
            } catch (Throwable t) {
                exportCollector.cleanup(t);
                throw t;
            }

            responseObserver.onNext(responseBuilder.build());
        }

        @Override
        public void close() {
            responseObserver.onCompleted();
        }
    }
}
