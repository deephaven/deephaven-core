package io.deephaven.server.hierarchicaltable;

import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.auth.codegen.impl.HierarchicalTableServiceContextualAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.extensions.barrage.util.ExportUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketResolverBase;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.server.table.ops.AggregationAdapter;
import io.deephaven.server.table.ops.FilterTableGrpcImpl;
import io.deephaven.server.table.ops.filter.FilterFactory;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.lang.Object;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.AbsoluteSortColumnConventions.baseColumnNameToAbsoluteName;
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
                                .map(AggregationAdapter::adapt)
                                .collect(Collectors.toList());
                        final boolean includeConstituents = request.getIncludeConstituents();
                        final Collection<ColumnName> groupByColumns = request.getGroupByColumnsList().stream()
                                .map(ColumnName::of)
                                .collect(Collectors.toList());
                        final RollupTable result = sourceTable.rollup(
                                aggregations, includeConstituents, groupByColumns);

                        final RollupTable transformedResult = authTransformation.transform(result);
                        safelyExecute(() -> {
                            responseObserver.onNext(RollupResponse.getDefaultInstance());
                            responseObserver.onCompleted();
                        });
                        return transformedResult;
                    });
        });
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
                        final TreeTable result = sourceTable.tree(
                                identifierColumn.name(), parentIdentifierColumn.name());

                        final TreeTable transformedResult = authTransformation.transform(result);
                        safelyExecute(() -> {
                            responseObserver.onNext(TreeResponse.getDefaultInstance());
                            responseObserver.onCompleted();
                        });
                        return transformedResult;
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

                        authWiring.checkPermissionApply(session.getAuthContext(), request,
                                List.of(inputHierarchicalTable.getSource()));

                        if (request.getFiltersCount() == 0 && request.getSortsCount() == 0) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No operations specified");
                        }
                        final Collection<Condition> finishedConditions = request.getFiltersCount() == 0
                                ? null
                                : FilterTableGrpcImpl.finishConditions(request.getFiltersList());
                        final Collection<SortColumn> translatedSorts = request.getSortsCount() == 0
                                ? null
                                : request.getSortsList().stream()
                                        .map(HierarchicalTableServiceGrpcImpl::translateSort)
                                        .collect(Collectors.toList());

                        final HierarchicalTable<?> result;
                        if (inputHierarchicalTable instanceof RollupTable) {
                            RollupTable rollupTable = (RollupTable) inputHierarchicalTable;
                            // Rollups only support filtering on the group-by columns, so we can safely use the
                            // aggregated node definition here.
                            final TableDefinition nodeDefinition =
                                    rollupTable.getNodeDefinition(RollupTable.NodeType.Aggregated);
                            if (finishedConditions != null) {
                                rollupTable = rollupTable.withFilters(finishedConditions.stream()
                                        .map(condition -> FilterFactory.makeFilter(nodeDefinition, condition))
                                        .collect(Collectors.toList()));
                            }
                            if (translatedSorts != null) {
                                RollupTable.NodeOperationsRecorder aggregatedSorts =
                                        rollupTable.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated);
                                aggregatedSorts = aggregatedSorts.sort(translatedSorts);
                                if (rollupTable.includesConstituents()) {
                                    final RollupTable.NodeOperationsRecorder constituentSorts = rollupTable
                                            .translateAggregatedNodeOperationsForConstituentNodes(aggregatedSorts);
                                    rollupTable = rollupTable.withNodeOperations(aggregatedSorts, constituentSorts);
                                } else {
                                    rollupTable = rollupTable.withNodeOperations(aggregatedSorts);
                                }
                            }
                            result = rollupTable;
                        } else if (inputHierarchicalTable instanceof TreeTable) {
                            TreeTable treeTable = (TreeTable) inputHierarchicalTable;
                            final TableDefinition nodeDefinition = treeTable.getNodeDefinition();
                            if (finishedConditions != null) {
                                treeTable = treeTable.withFilters(finishedConditions.stream()
                                        .map(condition -> FilterFactory.makeFilter(nodeDefinition, condition))
                                        .collect(Collectors.toList()));
                            }
                            if (translatedSorts != null) {
                                TreeTable.NodeOperationsRecorder treeSorts = treeTable.makeNodeOperationsRecorder();
                                treeSorts = treeSorts.sort(translatedSorts);
                                treeTable = treeTable.withNodeOperations(treeSorts);
                            }
                            result = treeTable;
                        } else {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Input is not a supported HierarchicalTable type");
                        }

                        final HierarchicalTable<?> transformedResult = authTransformation.transform(result);
                        safelyExecute(() -> {
                            responseObserver.onNext(HierarchicalTableApplyResponse.getDefaultInstance());
                            responseObserver.onCompleted();
                        });
                        return transformedResult;
                    });
        });
    }

    private static SortColumn translateSort(@NotNull final SortDescriptor sortDescriptor) {
        switch (sortDescriptor.getDirection()) {
            case DESCENDING:
                return SortColumn.desc(ColumnName.of(sortDescriptor.getIsAbsolute()
                        ? baseColumnNameToAbsoluteName(sortDescriptor.getColumnName())
                        : sortDescriptor.getColumnName()));
            case ASCENDING:
                return SortColumn.asc(ColumnName.of(sortDescriptor.getIsAbsolute()
                        ? baseColumnNameToAbsoluteName(sortDescriptor.getColumnName())
                        : sortDescriptor.getColumnName()));
            case UNKNOWN:
            case REVERSE:
            default:
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unsupported or unknown sort direction: " + sortDescriptor.getDirection());
        }
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
                        final Object target = targetExport.get();
                        final HierarchicalTableView targetExistingView = usedExisting
                                ? (HierarchicalTableView) target
                                : null;
                        final HierarchicalTable targetHierarchicalTable = usedExisting
                                ? targetExistingView.getHierarchicalTable()
                                : (HierarchicalTable) target;

                        authWiring.checkPermissionView(session.getAuthContext(), request, keyTable == null
                                ? List.of(targetHierarchicalTable.getSource())
                                : List.of(keyTable, targetHierarchicalTable.getSource()));

                        final HierarchicalTableView result;
                        if (usedExisting) {
                            if (keyTable != null) {
                                result = HierarchicalTableView.makeFromExistingView(
                                        targetExistingView,
                                        keyTable,
                                        request.getExpansions().hasKeyTableActionColumn()
                                                ? ColumnName.of(request.getExpansions().getKeyTableActionColumn())
                                                : null);
                            } else {
                                result = HierarchicalTableView.makeFromExistingView(targetExistingView);
                            }
                        } else {
                            if (keyTable != null) {
                                result = HierarchicalTableView.makeFromHierarchicalTable(
                                        targetHierarchicalTable,
                                        keyTable,
                                        request.getExpansions().hasKeyTableActionColumn()
                                                ? ColumnName.of(request.getExpansions().getKeyTableActionColumn())
                                                : null);
                            } else {
                                result = HierarchicalTableView.makeFromHierarchicalTable(targetHierarchicalTable);
                            }
                        }

                        final HierarchicalTableView transformedResult = authTransformation.transform(result);
                        safelyExecute(() -> {
                            responseObserver.onNext(HierarchicalTableViewResponse.getDefaultInstance());
                            responseObserver.onCompleted();
                        });
                        return transformedResult;
                    });
        });
    }

    @Override
    public void exportSource(
            @NotNull final HierarchicalTableSourceExportRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final SessionState.ExportObject<HierarchicalTable> hierarchicalTableExport = ticketRouter.resolve(
                    session, request.getHierarchicalTableId(), "exportSource.hierarchicalTableId");

            session.newExport(request.getResultTableId(), "exportSource.resultTableId")
                    .require(hierarchicalTableExport)
                    .onError(responseObserver)
                    .submit(() -> {
                        final HierarchicalTable<?> hierarchicalTable = hierarchicalTableExport.get();

                        final Table result = hierarchicalTable.getSource();
                        authWiring.checkPermissionExportSource(session.getAuthContext(), request, List.of(result));

                        final Table transformedResult = authTransformation.transform(result);
                        final ExportedTableCreationResponse response =
                                ExportUtil.buildTableCreationResponse(request.getResultTableId(), transformedResult);
                        safelyExecute(() -> {
                            responseObserver.onNext(response);
                            responseObserver.onCompleted();
                        });
                        return transformedResult;
                    });
        });
    }
}
