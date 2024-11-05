//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.hierarchicaltable;

import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;
import io.deephaven.auth.codegen.impl.HierarchicalTableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.AbsoluteSortColumnConventions;
import io.deephaven.engine.table.impl.BaseGridAttributes;
import io.deephaven.engine.table.impl.hierarchical.RollupTableImpl;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.extensions.barrage.util.ExportUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.*;
import io.deephaven.server.table.ops.AggregationAdapter;
import io.deephaven.server.table.ops.FilterTableGrpcImpl;
import io.deephaven.server.table.ops.filter.FilterFactory;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import java.lang.Object;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.AbsoluteSortColumnConventions.baseColumnNameToAbsoluteName;

public class HierarchicalTableServiceGrpcImpl extends HierarchicalTableServiceGrpc.HierarchicalTableServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(HierarchicalTableServiceGrpcImpl.class);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final HierarchicalTableServiceContextualAuthWiring authWiring;
    private final TicketResolver.Authorization authTransformation;

    @Inject
    public HierarchicalTableServiceGrpcImpl(
            @NotNull final TicketRouter ticketRouter,
            @NotNull final SessionService sessionService,
            @NotNull final AuthorizationProvider authorizationProvider) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.authWiring = authorizationProvider.getHierarchicalTableServiceContextualAuthWiring();
        this.authTransformation = authorizationProvider.getTicketResolverAuthorization();
    }

    @Override
    public void rollup(
            @NotNull final RollupRequest request,
            @NotNull final StreamObserver<RollupResponse> responseObserver) {
        validate(request);

        final SessionState session = sessionService.getCurrentSession();

        final String description = "HierarchicalTableService#rollup(table="
                + ticketRouter.getLogNameFor(request.getSourceTableId(), "sourceTableId") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<Table> sourceTableExport =
                    ticketRouter.resolve(session, request.getSourceTableId(), "sourceTableId");

            session.<RollupTable>newExport(request.getResultRollupTableId(), "resultRollupTableId")
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .require(sourceTableExport)
                    .onError(responseObserver)
                    .onSuccess((final RollupTable ignoredResult) -> GrpcUtil.safelyOnNextAndComplete(responseObserver,
                            RollupResponse.getDefaultInstance()))
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
                        if (transformedResult == null) {
                            throw Exceptions.statusRuntimeException(
                                    Code.FAILED_PRECONDITION, "Not authorized to rollup hierarchical table");
                        }
                        return transformedResult;
                    });
        }
    }

    private static void validate(@NotNull final RollupRequest request) {
        GrpcErrorHelper.checkHasField(request, RollupRequest.RESULT_ROLLUP_TABLE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, RollupRequest.SOURCE_TABLE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getResultRollupTableId());
        Common.validate(request.getSourceTableId());
        request.getAggregationsList().forEach(AggregationAdapter::validate);
    }

    @Override
    public void tree(
            @NotNull final TreeRequest request,
            @NotNull final StreamObserver<TreeResponse> responseObserver) {
        validate(request);

        final SessionState session = sessionService.getCurrentSession();

        final String description = "HierarchicalTableService#tree(table="
                + ticketRouter.getLogNameFor(request.getSourceTableId(), "sourceTableId") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<Table> sourceTableExport =
                    ticketRouter.resolve(session, request.getSourceTableId(), "sourceTableId");

            session.<TreeTable>newExport(request.getResultTreeTableId(), "resultTreeTableId")
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .require(sourceTableExport)
                    .onError(responseObserver)
                    .onSuccess((final TreeTable ignoredResult) -> GrpcUtil.safelyOnNextAndComplete(responseObserver,
                            TreeResponse.getDefaultInstance()))
                    .submit(() -> {
                        final Table sourceTable = sourceTableExport.get();

                        authWiring.checkPermissionTree(session.getAuthContext(), request, List.of(sourceTable));

                        final ColumnName identifierColumn = ColumnName.of(request.getIdentifierColumn());
                        final ColumnName parentIdentifierColumn = ColumnName.of(request.getParentIdentifierColumn());

                        final Table sourceTableToUse;
                        if (request.getPromoteOrphans()) {
                            sourceTableToUse = TreeTable.promoteOrphans(
                                    sourceTable, identifierColumn.name(), parentIdentifierColumn.name());
                        } else {
                            sourceTableToUse = sourceTable;
                        }

                        final TreeTable result = sourceTableToUse.tree(
                                identifierColumn.name(), parentIdentifierColumn.name());

                        final TreeTable transformedResult = authTransformation.transform(result);
                        if (transformedResult == null) {
                            throw Exceptions.statusRuntimeException(
                                    Code.FAILED_PRECONDITION, "Not authorized to tree hierarchical table");
                        }
                        return transformedResult;
                    });
        }
    }

    private static void validate(@NotNull final TreeRequest request) {
        GrpcErrorHelper.checkHasField(request, TreeRequest.RESULT_TREE_TABLE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, TreeRequest.SOURCE_TABLE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, TreeRequest.IDENTIFIER_COLUMN_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, TreeRequest.PARENT_IDENTIFIER_COLUMN_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getResultTreeTableId());
        Common.validate(request.getSourceTableId());
    }

    @Override
    public void apply(
            @NotNull final HierarchicalTableApplyRequest request,
            @NotNull final StreamObserver<HierarchicalTableApplyResponse> responseObserver) {
        validate(request);

        final SessionState session = sessionService.getCurrentSession();

        final String description = "HierarchicalTableService#apply(table="
                + ticketRouter.getLogNameFor(request.getInputHierarchicalTableId(), "inputHierarchicalTableId") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<HierarchicalTable<?>> inputHierarchicalTableExport =
                    ticketRouter.resolve(session, request.getInputHierarchicalTableId(), "inputHierarchicalTableId");

            session.<HierarchicalTable<?>>newExport(request.getResultHierarchicalTableId(), "resultHierarchicalTableId")
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .require(inputHierarchicalTableExport)
                    .onError(responseObserver)
                    .onSuccess((final HierarchicalTable<?> ignoredResult) -> GrpcUtil.safelyOnNextAndComplete(
                            responseObserver,
                            HierarchicalTableApplyResponse.getDefaultInstance()))
                    .submit(() -> {
                        final HierarchicalTable<?> inputHierarchicalTable = inputHierarchicalTableExport.get();

                        authWiring.checkPermissionApply(session.getAuthContext(), request,
                                List.of(inputHierarchicalTable.getSource()));

                        if (request.getFiltersCount() == 0 && request.getSortsCount() == 0) {
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "No operations specified");
                        }
                        final Collection<Condition> finishedConditions = request.getFiltersCount() == 0
                                ? null
                                : FilterTableGrpcImpl.finishConditions(request.getFiltersList());
                        final Collection<SortColumn> translatedSorts =
                                translateAndValidateSorts(request, (BaseGridAttributes<?, ?>) inputHierarchicalTable);

                        final HierarchicalTable<?> result;
                        if (inputHierarchicalTable instanceof RollupTable) {
                            RollupTable rollupTable = (RollupTable) inputHierarchicalTable;
                            // Rollups only support filtering on the group-by columns, so we can safely use the
                            // aggregated node definition here.
                            final TableDefinition nodeDefinition =
                                    rollupTable.getNodeDefinition(RollupTable.NodeType.Aggregated);
                            if (finishedConditions != null) {
                                final Collection<? extends WhereFilter> filters =
                                        makeWhereFilters(finishedConditions, nodeDefinition);
                                RollupTableImpl.initializeAndValidateFilters(
                                        rollupTable.getSource(),
                                        rollupTable.getGroupByColumns(),
                                        filters,
                                        message -> Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, message));
                                rollupTable = rollupTable.withFilter(Filter.and(filters));
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
                                treeTable = treeTable
                                        .withFilter(Filter.and(makeWhereFilters(finishedConditions, nodeDefinition)));
                            }
                            if (translatedSorts != null) {
                                TreeTable.NodeOperationsRecorder treeSorts = treeTable.makeNodeOperationsRecorder();
                                treeSorts = treeSorts.sort(translatedSorts);
                                treeTable = treeTable.withNodeOperations(treeSorts);
                            }
                            result = treeTable;
                        } else {
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Input is not a supported HierarchicalTable type");
                        }

                        final HierarchicalTable<?> transformedResult = authTransformation.transform(result);
                        if (transformedResult == null) {
                            throw Exceptions.statusRuntimeException(
                                    Code.FAILED_PRECONDITION, "Not authorized to apply to hierarchical table");
                        }
                        return transformedResult;
                    });
        }
    }

    private static void validate(@NotNull final HierarchicalTableApplyRequest request) {
        GrpcErrorHelper.checkHasField(request, HierarchicalTableApplyRequest.RESULT_HIERARCHICAL_TABLE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, HierarchicalTableApplyRequest.INPUT_HIERARCHICAL_TABLE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getResultHierarchicalTableId());
        Common.validate(request.getInputHierarchicalTableId());
    }

    @NotNull
    private static List<WhereFilter> makeWhereFilters(
            @NotNull final Collection<Condition> finishedConditions,
            @NotNull final TableDefinition nodeDefinition) {
        return finishedConditions.stream()
                .map(condition -> FilterFactory.makeFilter(nodeDefinition, condition))
                .collect(Collectors.toList());
    }

    @Nullable
    private static Collection<SortColumn> translateAndValidateSorts(
            @NotNull final HierarchicalTableApplyRequest request,
            @NotNull final BaseGridAttributes<?, ?> inputHierarchicalTable) {
        if (request.getSortsCount() == 0) {
            return null;
        }
        final Collection<SortColumn> translatedSorts = request.getSortsList().stream()
                .map(HierarchicalTableServiceGrpcImpl::translateSort)
                .collect(Collectors.toList());
        final Set<String> sortableColumnNames = inputHierarchicalTable.getSortableColumns();
        if (sortableColumnNames != null) {
            if (sortableColumnNames.isEmpty()) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Sorting is not supported on this hierarchical table");
            }
            final Collection<String> unavailableSortColumnNames = translatedSorts.stream()
                    .map(sc -> AbsoluteSortColumnConventions.stripAbsoluteColumnName(sc.column().name()))
                    .filter(scn -> !sortableColumnNames.contains(scn))
                    .collect(Collectors.toList());
            if (!unavailableSortColumnNames.isEmpty()) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Sorting attempted on restricted column(s): "
                                + unavailableSortColumnNames.stream().collect(Collectors.joining(", ", "[", "]"))
                                + ", available column(s) for sorting are: "
                                + sortableColumnNames.stream().collect(Collectors.joining(", ", "[", "]")));
            }
        }
        return translatedSorts;
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
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unsupported or unknown sort direction: " + sortDescriptor.getDirection());
        }
    }

    @Override
    public void view(
            @NotNull final HierarchicalTableViewRequest request,
            @NotNull final StreamObserver<HierarchicalTableViewResponse> responseObserver) {
        validate(request);

        final SessionState session = sessionService.getCurrentSession();

        final boolean usedExisting;
        final Ticket targetTicket;
        switch (request.getTargetCase()) {
            case HIERARCHICAL_TABLE_ID:
                usedExisting = false;
                targetTicket = request.getHierarchicalTableId();
                break;
            case EXISTING_VIEW_ID:
                usedExisting = true;
                targetTicket = request.getExistingViewId();
                break;
            case TARGET_NOT_SET:
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "No target specified");
        }

        final String description = "HierarchicalTableService#view(table="
                + ticketRouter.getLogNameFor(targetTicket, "targetTableId") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportBuilder<HierarchicalTableView> resultExportBuilder =
                    session.newExport(request.getResultViewId(), "resultViewId");

            final SessionState.ExportObject<?> targetExport =
                    ticketRouter.resolve(session, targetTicket, "targetTableId");

            final SessionState.ExportObject<Table> keyTableExport;
            if (request.hasExpansions()) {
                keyTableExport = ticketRouter.resolve(
                        session, request.getExpansions().getKeyTableId(), "expansions.keyTableId");
                resultExportBuilder.require(targetExport, keyTableExport);
            } else {
                keyTableExport = null;
                resultExportBuilder.require(targetExport);
            }

            resultExportBuilder
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .onError(responseObserver)
                    .onSuccess((final HierarchicalTableView ignoredResult) -> GrpcUtil.safelyOnNextAndComplete(
                            responseObserver,
                            HierarchicalTableViewResponse.getDefaultInstance()))
                    .submit(() -> {
                        final Table keyTable = keyTableExport == null ? null : keyTableExport.get();
                        final Object target = targetExport.get();
                        final HierarchicalTableView targetExistingView = usedExisting
                                ? (HierarchicalTableView) target
                                : null;
                        final HierarchicalTable<?> targetHierarchicalTable = usedExisting
                                ? targetExistingView.getHierarchicalTable()
                                : (HierarchicalTable<?>) target;

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
                        if (transformedResult == null) {
                            throw Exceptions.statusRuntimeException(
                                    Code.FAILED_PRECONDITION, "Not authorized to view hierarchical table");
                        }
                        return transformedResult;
                    });
        }
    }

    private static void validate(@NotNull final HierarchicalTableViewRequest request) {
        GrpcErrorHelper.checkHasField(request, HierarchicalTableViewRequest.RESULT_VIEW_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasOneOf(request, "target");
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        switch (request.getTargetCase()) {
            case HIERARCHICAL_TABLE_ID:
                Common.validate(request.getHierarchicalTableId());
                break;
            case EXISTING_VIEW_ID:
                Common.validate(request.getExistingViewId());
                break;
            case TARGET_NOT_SET:
                // noinspection ThrowableNotThrown
                Assert.statementNeverExecuted("No target specified, despite prior validation");
                break;
            default:
                throw Exceptions.statusRuntimeException(Code.INTERNAL, String.format("%s has unexpected target case %s",
                        request.getDescriptorForType().getFullName(), request.getTargetCase()));
        }
    }

    @Override
    public void exportSource(
            @NotNull final HierarchicalTableSourceExportRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        validate(request);

        final SessionState session = sessionService.getCurrentSession();

        final String description = "HierarchicalTableService#exportSource(table="
                + ticketRouter.getLogNameFor(request.getHierarchicalTableId(), "hierarchicalTableId") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<HierarchicalTable<?>> hierarchicalTableExport =
                    ticketRouter.resolve(session, request.getHierarchicalTableId(), "hierarchicalTableId");

            session.<Table>newExport(request.getResultTableId(), "resultTableId")
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .require(hierarchicalTableExport)
                    .onError(responseObserver)
                    .onSuccess((final Table transformedResult) -> GrpcUtil.safelyOnNextAndComplete(responseObserver,
                            ExportUtil.buildTableCreationResponse(request.getResultTableId(), transformedResult)))
                    .submit(() -> {
                        final HierarchicalTable<?> hierarchicalTable = hierarchicalTableExport.get();

                        final Table result = hierarchicalTable.getSource();
                        authWiring.checkPermissionExportSource(session.getAuthContext(), request, List.of(result));

                        final Table transformedResult = authTransformation.transform(result);
                        if (transformedResult == null) {
                            throw Exceptions.statusRuntimeException(
                                    Code.FAILED_PRECONDITION,
                                    "Not authorized to export source from hierarchical table");
                        }
                        return transformedResult;
                    });
        }
    }

    private static void validate(@NotNull final HierarchicalTableSourceExportRequest request) {
        GrpcErrorHelper.checkHasField(request, HierarchicalTableSourceExportRequest.RESULT_TABLE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, HierarchicalTableSourceExportRequest.HIERARCHICAL_TABLE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getResultTableId());
        Common.validate(request.getHierarchicalTableId());
    }
}
