package io.deephaven.server.hierarchicaltable;

import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.TicketRouter;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;

public class HierarchicalTableServiceGrpcImpl extends HierarchicalTableServiceGrpc.HierarchicalTableServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(HierarchicalTableServiceGrpcImpl.class);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final UpdateGraphProcessor updateGraphProcessor;

    @Inject
    public HierarchicalTableServiceGrpcImpl(
            @NotNull final TicketRouter ticketRouter,
            @NotNull final SessionService sessionService,
            @NotNull final UpdateGraphProcessor updateGraphProcessor) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.updateGraphProcessor = updateGraphProcessor;
    }

    @Override
    public void rollup(
            @NotNull final RollupRequest request,
            @NotNull final StreamObserver<RollupResponse> responseObserver) {
        // TODO-RWC: IMPLEMENT ME
        super.rollup(request, responseObserver);
    }

    @Override
    public void tree(
            @NotNull final TreeRequest request,
            @NotNull final StreamObserver<TreeResponse> responseObserver) {
        // TODO-RWC: IMPLEMENT ME
        super.tree(request, responseObserver);
    }

    @Override
    public void exportSource(
            @NotNull final HierarchicalTableSourceExportRequest request,
            @NotNull final StreamObserver<HierarchicalTableSourceExportResponse> responseObserver) {
        // TODO-RWC: IMPLEMENT ME
        super.exportSource(request, responseObserver);
    }

    @Override
    public void view(
            @NotNull final HierarchicalTableViewRequest request,
            @NotNull final StreamObserver<HierarchicalTableViewResponse> responseObserver) {
        // TODO-RWC: IMPLEMENT ME
        super.view(request, responseObserver);
    }
}
