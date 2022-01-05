package io.deephaven.server.object;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil.ExposedByteArrayOutputStream;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.Exporter.Export;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.proto.backplane.grpc.FetchObjectRequest2;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse2;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse2.Builder;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.session.TicketRouter;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.IOException;
import java.util.Objects;

public class ObjectServiceGrpcImpl extends ObjectServiceGrpc.ObjectServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ObjectServiceGrpcImpl.class);

    private final SessionService sessionService;
    private final TicketRouter ticketRouter;
    private final ObjectTypeLookup objectTypeLookup;

    @Inject
    public ObjectServiceGrpcImpl(SessionService sessionService, TicketRouter ticketRouter, ObjectTypeLookup objectTypeLookup) {
        this.sessionService = Objects.requireNonNull(sessionService);
        this.ticketRouter = Objects.requireNonNull(ticketRouter);
        this.objectTypeLookup = Objects.requireNonNull(objectTypeLookup);
    }

    @Override
    public void fetchObject2(FetchObjectRequest2 request, StreamObserver<FetchObjectResponse2> responseObserver) {
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
                        final FetchObjectResponse2 response = serialize(session, o);
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return null;
                    });
        });
    }

    private FetchObjectResponse2 serialize(SessionState state, Object object) throws IOException {
        final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream();
        final ObjectType type = objectTypeLookup.findObjectType(object).orElseThrow(() -> noTypeException(object));
        final Builder builder = FetchObjectResponse2.newBuilder().setType(type.name());
        final ExportCollector exportCollector = new ExportCollector(state, builder);
        type.writeTo(exportCollector, object, out);
        final ByteString data = ByteStringAccess.wrap(out.peekBuffer(), 0, out.size());
        return builder.setData(data).build();
    }

    private static IllegalArgumentException noTypeException(Object o) {
        return new IllegalArgumentException(
                "No type registered for object class=" + o.getClass().getName() + ", value=" + o);
    }

    private static final class ExportCollector implements Exporter {

        private final SessionState sessionState;
        private final Builder builder;
        private final Thread thread;

        public ExportCollector(SessionState sessionState, Builder builder) {
            this.sessionState = Objects.requireNonNull(sessionState);
            this.builder = Objects.requireNonNull(builder);
            this.thread = Thread.currentThread();
        }

        @Override
        public Export newServerSideExport(Object object) {
            if (thread != Thread.currentThread()) {
                throw new IllegalStateException("Should only create exports on the calling thread");
            }
            final ExportObject<?> exportObject = sessionState.newServerSideExport(object);
            builder.addExportId(exportObject.getExportId());
            return new ExportImpl(exportObject);
        }
    }

    private static final class ExportImpl implements Export {
        private final ExportObject<?> export;

        public ExportImpl(ExportObject<?> export) {
            this.export = Objects.requireNonNull(export);
        }

        @Override
        public Ticket id() {
            return export.getExportId();
        }
    }
}
