package io.deephaven.server.object;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.engine.table.Table;
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
import io.deephaven.proto.backplane.grpc.Ticket;
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

public class ObjectServiceGrpcImpl extends ObjectServiceGrpc.ObjectServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ObjectServiceGrpcImpl.class);

    private final SessionService sessionService;
    private final TicketRouter ticketRouter;
    private final ObjectTypeLookup objectTypeLookup;

    @Inject
    public ObjectServiceGrpcImpl(SessionService sessionService, TicketRouter ticketRouter,
            ObjectTypeLookup objectTypeLookup) {
        this.sessionService = Objects.requireNonNull(sessionService);
        this.ticketRouter = Objects.requireNonNull(ticketRouter);
        this.objectTypeLookup = Objects.requireNonNull(objectTypeLookup);
    }

    @Override
    public void fetchObject(FetchObjectRequest request, StreamObserver<FetchObjectResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            if (request.getSourceId().getTicket().isEmpty()) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "No sourceId supplied");
            }
            final SessionState.ExportObject<Object> object = ticketRouter.resolve(
                    session, request.getSourceId(), "sourceId");
            session.nonExport()
                    .require(object)
                    .onError(responseObserver)
                    .submit(() -> {
                        final Object o = object.get();
                        final FetchObjectResponse response = serialize(session, o);
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return null;
                    });
        });
    }

    private FetchObjectResponse serialize(SessionState state, Object object) throws IOException {
        final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream();
        final ObjectType objectType =
                objectTypeLookup.findObjectType(object).orElseThrow(() -> noTypeException(object));
        final ExportCollector exportCollector = new ExportCollector(state);
        try {
            objectType.writeTo(exportCollector, object, out);
            final Builder builder = FetchObjectResponse.newBuilder()
                    .setType(objectType.name())
                    .setData(ByteStringAccess.wrap(out.peekBuffer(), 0, out.size()));
            for (ExportObject<?> export : exportCollector.exports()) {
                final String exportType = findType(export.get());
                final Ticket exportId = export.getExportId();
                builder.addExportId(TypedTicket.newBuilder()
                        .setType(exportType)
                        .setTicket(exportId.getTicket())
                        .build());
            }
            return builder.build();
        } catch (Throwable t) {
            cleanup(exportCollector, t);
            throw t;
        }
    }

    // todo consolidate logic
    private String findType(Object o) {
        if (o instanceof Table) {
            return "Table";
        }
        return objectTypeLookup.findObjectType(o).map(ObjectType::name).orElse("");
    }

    private static void cleanup(ExportCollector exportCollector, Throwable t) {
        for (ExportObject<?> export : exportCollector.exports()) {
            try {
                export.release();
            } catch (Throwable inner) {
                t.addSuppressed(inner);
            }
        }
    }

    private static IllegalArgumentException noTypeException(Object o) {
        return new IllegalArgumentException(
                "No type registered for object class=" + o.getClass().getName() + ", value=" + o);
    }

    // Make private after
    // TODO(deephaven-core#1784): Remove fetchFigure RPC
    public static final class ExportCollector implements Exporter {

        private final SessionState sessionState;
        private final Thread thread;
        private final List<ExportObject<?>> exports;

        public ExportCollector(SessionState sessionState) {
            this.sessionState = Objects.requireNonNull(sessionState);
            this.thread = Thread.currentThread();
            this.exports = new ArrayList<>();
        }

        public List<ExportObject<?>> exports() {
            return exports;
        }

        @Override
        public Reference newServerSideReference(Object object) {
            if (thread != Thread.currentThread()) {
                throw new IllegalStateException("Should only create new references on the calling thread");
            }
            final ExportObject<?> exportObject = sessionState.newServerSideExport(object);
            exports.add(exportObject);
            return new ReferenceImpl(exportObject);
        }
    }

    private static final class ReferenceImpl implements Reference {
        private final ExportObject<?> export;

        public ReferenceImpl(ExportObject<?> export) {
            this.export = Objects.requireNonNull(export);
        }

        @Override
        public String type() {
            return null;
        }

        @Override
        public byte[] ticket() {
            return export.getExportIdBytes();
        }

        @Override
        public Ticket id() {
            return export.getExportId();
        }
    }
}
