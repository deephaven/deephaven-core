package io.deephaven.server.hierarchicaltable;

import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.auth.codegen.impl.HierarchicalTableServiceContextualAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
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
import java.util.List;
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

            final SessionState.ExportObject<Table> sourceTableExport = ticketRouter.resolve(
                    session, request.getSourceTableId(), "rollup.sourceTableId");

            session.newExport(request.getResultRollupTableId(), "rollup.resultRollupTableId")
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

            final SessionState.ExportObject<Table> sourceTableExport = ticketRouter.resolve(
                    session, request.getSourceTableId(), "tree.sourceTableId");

            session.newExport(request.getResultTreeTableId(), "tree.resultTreeTableId")
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
    public void apply(
            @NotNull final HierarchicalTableApplyRequest request,
            @NotNull final StreamObserver<HierarchicalTableApplyResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final SessionState.ExportObject<HierarchicalTable> inputHierarchicalTableExport = ticketRouter.resolve(
                    session, request.getInputHierarchicalTableId(), "apply.inputHierarchicalTableId");

            session.newExport(request.getResultHierarchicalTableId(), "apply.resultHierarchicalTableId")
                    .require(inputHierarchicalTableExport)
                    .onError(responseObserver)
                    .submit(() -> {
                        final HierarchicalTable<?> inputHierarchicalTable = inputHierarchicalTableExport.get();
                        // TODO-RWC: Get auth wiring updated and integrated here.
                        // authWiring.checkPermissionTree(session.getAuthContext(), request, List.of(sourceTable));
                        if (request.getFiltersCount() == 0 && request.getSortsCount() == 0) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No operations specified");
                        }
                        final HierarchicalTable<?> result;
                        if (inputHierarchicalTable instanceof RollupTable) {
                            RollupTable rollupTable = (RollupTable) inputHierarchicalTable;
                            if (request.getFiltersCount() > 0) {
                                // TODO-RWC: Convert and apply operations
                                // rollupTable = rollupTable.withFilters(request.getFiltersList());
                            }
                            result = rollupTable;
                        } else if (inputHierarchicalTable instanceof TreeTable) {
                            TreeTable treeTable = (TreeTable) inputHierarchicalTable;
                            // TODO-RWC: Convert and apply operations
                            result = treeTable;
                        } else {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Input is not a supported HierarchicalTable type");
                        }
                        safelyExecute(() -> {
                            responseObserver.onNext(HierarchicalTableApplyResponse.getDefaultInstance());
                            responseObserver.onCompleted();
                        });
                        return result;
                    });
        });
    }

    @Override
    public void view(
            @NotNull final HierarchicalTableViewRequest request,
            @NotNull final StreamObserver<HierarchicalTableViewResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            if (!request.hasHierarchicalTableId() && !request.hasExistingViewId()) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No target specified");
            }
            // TODO-RWC: Get auth wiring updated and integrated here.

            final SessionState.ExportBuilder<HierarchicalTableView> resultExportBuilder =
                    session.newExport(request.getResultViewId(), "view.resultViewId");

            final boolean usedExisting = request.hasExistingViewId();
            final Ticket targetTicket = usedExisting ? request.getExistingViewId() : request.getHierarchicalTableId();
            final SessionState.ExportObject<?> targetExport = ticketRouter.resolve(
                    session, targetTicket, "view.target");

            final SessionState.ExportObject<Table> keyTableExport;
            if (request.hasExpansions()) {
                keyTableExport = ticketRouter.resolve(
                        session, request.getExpansions().getKeyTableId(), "view.expansions.keyTableId");
                resultExportBuilder.require(targetExport, keyTableExport);
            } else {
                keyTableExport = null;
                resultExportBuilder.require(targetExport);
            }

            resultExportBuilder.onError(responseObserver)
                    .submit(() -> {
                        final Table keyTable = keyTableExport == null ? null : keyTableExport.get();
                        final HierarchicalTableView result;
                        if (usedExisting) {
                            final HierarchicalTableView existingView = (HierarchicalTableView) targetExport.get();
                            if (keyTable != null) {
                                result = HierarchicalTableView.makeFromExistingView(
                                        existingView,
                                        keyTable,
                                        request.getExpansions().hasKeyTableActionColumn()
                                                ? ColumnName.of(request.getExpansions().getKeyTableActionColumn())
                                                : null);
                            } else {
                                result = HierarchicalTableView.makeFromExistingView(existingView);
                            }
                        } else {
                            final HierarchicalTable<?> hierarchicalTable = (HierarchicalTable<?>) targetExport.get();
                            if (keyTable != null) {
                                result = HierarchicalTableView.makeFromHierarchicalTable(
                                        hierarchicalTable,
                                        keyTable,
                                        request.getExpansions().hasKeyTableActionColumn()
                                                ? ColumnName.of(request.getExpansions().getKeyTableActionColumn())
                                                : null);
                            } else {
                                result = HierarchicalTableView.makeFromHierarchicalTable(hierarchicalTable);
                            }
                        }
                        safelyExecute(() -> {
                            responseObserver.onNext(HierarchicalTableViewResponse.getDefaultInstance());
                            responseObserver.onCompleted();
                        });
                        return result;
                    });
        });
    }
}
