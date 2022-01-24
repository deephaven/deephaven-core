package io.deephaven.server.object;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil.ExposedByteArrayOutputStream;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectType.Exporter;
import io.deephaven.plugin.type.ObjectType.Exporter.Reference;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.proto.backplane.grpc.FetchObjectRequest;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse.Builder;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.session.TicketRouter;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;

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

    @Override
    public void fetchObject(FetchObjectRequest request, StreamObserver<FetchObjectResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            final String type = request.getSourceId().getType();
            if (type.isEmpty()) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No type supplied");
            }
            if (request.getSourceId().getTicket().getTicket().isEmpty()) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No ticket supplied");
            }
            final SessionState.ExportObject<Object> object = ticketRouter.resolve(
                    session, request.getSourceId().getTicket(), "sourceId");
            session.nonExport()
                    .require(object)
                    .onError(responseObserver)
                    .submit(() -> {
                        final Object o = object.get();
                        final FetchObjectResponse response = serialize(type, session, o);
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return null;
                    });
        });
    }

    private FetchObjectResponse serialize(String expectedType, SessionState state, Object object) throws IOException {
        final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream();
        // TODO(deephaven-core#1872): Optimize ObjectTypeLookup
        final Optional<ObjectType> o = objectTypeLookup.findObjectType(object);
        if (o.isEmpty()) {
            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                    String.format("No ObjectType found, expected type '%s'", expectedType));
        }
        final ObjectType objectType = o.get();
        if (!expectedType.equals(objectType.name())) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, String.format(
                    "Unexpected ObjectType, expected type '%s', actual type '%s'", expectedType, objectType.name()));
        }
        final ExportCollector exportCollector = new ExportCollector(state);
        try {
            objectType.writeTo(exportCollector, object, out);
            final Builder builder = FetchObjectResponse.newBuilder()
                    .setType(objectType.name())
                    .setData(ByteStringAccess.wrap(out.peekBuffer(), 0, out.size()));
            for (ReferenceImpl ref : exportCollector.refs()) {
                builder.addTypedExportId(ref.typedTicket());
            }
            return builder.build();
        } catch (Throwable t) {
            cleanup(exportCollector, t);
            throw t;
        }
    }

    private static void cleanup(ExportCollector exportCollector, Throwable t) {
        for (ReferenceImpl ref : exportCollector.refs()) {
            try {
                ref.export.release();
            } catch (Throwable inner) {
                t.addSuppressed(inner);
            }
        }
    }

    private static boolean referenceEquality(Object t, Object u) {
        return t == u;
    }

    final class ExportCollector implements Exporter {

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

        @Override
        public Optional<Reference> reference(Object object, boolean allowUnknownType, boolean forceNew) {
            return reference(object, allowUnknownType, forceNew, ObjectServiceGrpcImpl::referenceEquality);
        }

        @Override
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
}
