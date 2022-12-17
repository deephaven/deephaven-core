package io.deephaven.server.hierarchicaltable;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.auth.codegen.impl.HierarchicalTableServiceContextualAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketResolverBase;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.server.table.ops.ComboAggregateGrpcImpl;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyExecute;

public class HierarchicalTableServiceGrpcImpl extends HierarchicalTableServiceGrpc.HierarchicalTableServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(HierarchicalTableServiceGrpcImpl.class);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final HierarchicalTableServiceContextualAuthWiring authWiring;
    private final TicketResolverBase.AuthTransformation authTransformation;

    @Inject
    public HierarchicalTableServiceGrpcImpl(
            @NotNull final TicketRouter ticketRouter,
            @NotNull final SessionService sessionService,
            @NotNull final AuthorizationProvider authorizationProvider) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.authWiring = authorizationProvider.getHierarchicalTableServiceContextualAuthWiring();
        this.authTransformation = authorizationProvider.getTicketTransformation();
    }

    @Override
    public void rollup(
            @NotNull final RollupRequest request,
            @NotNull final StreamObserver<RollupResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final SessionState.ExportObject<Table> sourceTableExport =
                    ticketRouter.resolve(session, request.getSourceId(), "rollup sourceId");

            session.newExport(request.getResultViewId(), "rollup resultViewId")
                    .require(sourceTableExport)
                    .onError(responseObserver)
                    .submit(() -> {
                        final Table sourceTable = sourceTableExport.get();
                        authWiring.checkPermissionRollup(session.getAuthContext(), request, List.of(sourceTable));
                        final Collection<? extends Aggregation> aggregations = request.getAggregationsList().stream()
                                .map(HierarchicalTableServiceGrpcImpl::makeAggregation)
                                .collect(Collectors.toList());
                        final boolean includeConstituents = request.getIncludeConstituents();
                        final Collection<ColumnName> groupByColumns = request.getGroupByColumnsList().stream()
                                .map(ColumnName::of)
                                .collect(Collectors.toList());
                        final RollupTable rollupTable = sourceTable.rollup(
                                aggregations, includeConstituents, groupByColumns);
                        safelyExecute(() -> {
                            responseObserver.onNext(RollupResponse.getDefaultInstance());
                            responseObserver.onCompleted();
                        });
                        return rollupTable;
                    });
        });
    }

    private static String[] extractAggColumnPairs(@NotNull final ComboAggregateRequest.Aggregate agg) {
        if (agg.getMatchPairsCount() == 0) {
            throw new UnsupportedOperationException("Column aggregations must specify column name input/output pairs");
        }
        return agg.getMatchPairsList().toArray(String[]::new);
    }

    private static Aggregation makeAggregation(@NotNull final ComboAggregateRequest.Aggregate agg) {
        return ComboAggregateGrpcImpl.makeAggregation(agg, HierarchicalTableServiceGrpcImpl::extractAggColumnPairs);
    }

    @Override
    public void tree(
            @NotNull final TreeRequest request,
            @NotNull final StreamObserver<TreeResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final SessionState.ExportObject<Table> sourceTableExport =
                    ticketRouter.resolve(session, request.getSourceId(), "tree sourceId");

            session.newExport(request.getResultViewId(), "tree resultViewId")
                    .require(sourceTableExport)
                    .onError(responseObserver)
                    .submit(() -> {
                        final Table sourceTable = sourceTableExport.get();
                        authWiring.checkPermissionTree(session.getAuthContext(), request, List.of(sourceTable));
                        final ColumnName identifierColumn = ColumnName.of(request.getIdentifierColumn());
                        final ColumnName parentIdentifierColumn = ColumnName.of(request.getParentIdentifierColumn());
                        final TreeTable treeTable = sourceTable.tree(
                                identifierColumn.name(), parentIdentifierColumn.name());
                        safelyExecute(() -> {
                            responseObserver.onNext(TreeResponse.getDefaultInstance());
                            responseObserver.onCompleted();
                        });
                        return treeTable;
                    });
        });
    }

    @Override
    public void exportSource(
            @NotNull final HierarchicalTableSourceExportRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
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
