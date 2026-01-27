//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationDescriptions;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.BaseTable.CopyAttributeOperation;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortOperation;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.AbsoluteSortColumnConventions.*;
import static io.deephaven.engine.table.impl.BaseTable.shouldCopyAttribute;
import static io.deephaven.engine.table.impl.by.AggregationProcessor.EXPOSED_GROUP_ROW_SETS;
import static io.deephaven.engine.table.impl.by.AggregationProcessor.getRowLookup;
import static io.deephaven.engine.table.impl.by.AggregationRowLookup.DEFAULT_UNKNOWN_ROW;
import static io.deephaven.engine.table.impl.by.AggregationRowLookup.EMPTY_KEY;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_COLUMN_SUFFIX;
import static io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl.LevelExpandable.All;
import static io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl.LevelExpandable.None;
import static io.deephaven.engine.table.impl.hierarchical.RollupNodeKeySource.ROOT_NODE_KEY;
import static io.deephaven.engine.table.impl.hierarchical.RollupNodeKeySource.ROOT_NODE_DEPTH;
import static io.deephaven.engine.table.impl.sources.ReinterpretUtils.maybeConvertToPrimitive;

/**
 * {@link RollupTable} implementation.
 */
public class RollupTableImpl extends HierarchicalTableImpl<RollupTable, RollupTableImpl> implements RollupTable {

    private static final int FIRST_AGGREGATED_COLUMN_INDEX = EXTRA_COLUMN_COUNT;
    private static final ColumnName ROLLUP_COLUMN = ColumnName.of(ROLLUP_COLUMN_SUFFIX);

    /**
     * The "root" node ID: depth 0, row position 0.
     */
    static final long ROOT_NODE_ID = makeNodeId(ROOT_NODE_DEPTH, 0);
    /**
     * The "null" node ID: negative depth, negative row position.
     */
    static final long NULL_NODE_ID = makeNodeId(-1, -1);

    private final Collection<? extends Aggregation> aggregations;
    private final boolean includesConstituents;
    private final Collection<? extends ColumnName> groupByColumns;

    private final int numLevels;
    private final int constituentDepth;
    private final QueryTable[] levelTables;
    private final AggregationRowLookup[] levelRowLookups;
    private final ColumnSource<Table>[] levelNodeTableSources;

    /**
     * When {@link #withFilter(Filter)} is called we apply the filters to the lowest level and re-aggregate. We need to
     * keep them around so that when we rebase to a new table we can re-apply them and preserve the semantics of our
     * chain of rollup operations.
     */
    @Nullable
    private final WhereFilter[] rollupKeyFilters;

    private final TableDefinition aggregatedNodeDefinition;
    private final RollupNodeOperationsRecorder aggregatedNodeOperations;
    private final TableDefinition constituentNodeDefinition;
    private final RollupNodeOperationsRecorder constituentNodeOperations;
    private final List<ColumnDefinition<?>> availableColumnDefinitions;

    private RollupTableImpl(
            @NotNull final Map<String, Object> initialAttributes,
            @NotNull final QueryTable source,
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includesConstituents,
            @NotNull final Collection<? extends ColumnName> groupByColumns,
            @NotNull final QueryTable[] levelTables,
            @NotNull final AggregationRowLookup[] levelRowLookups,
            @NotNull final ColumnSource<Table>[] levelNodeTableSources,
            @Nullable final TableDefinition aggregatedNodeDefinition,
            @Nullable final RollupNodeOperationsRecorder aggregatedNodeOperations,
            @Nullable final TableDefinition constituentNodeDefinition,
            @Nullable final RollupNodeOperationsRecorder constituentNodeOperations,
            @Nullable final WhereFilter[] rollupKeyFilters,
            @Nullable final List<ColumnDefinition<?>> availableColumnDefinitions) {
        super(initialAttributes, source, levelTables[0]);

        this.aggregations = aggregations;

        this.includesConstituents = includesConstituents;
        this.groupByColumns = groupByColumns;

        // One level for the "base" aggregation (which may include constituents, but we don't count them as a "rollup"
        // level), and one more for each group-by key as its removed for reaggregation.
        // Note that we don't count the "root" as a level in this accounting; the root is levelTables[0], and its
        // children (depth 1, at level 0), are found in levelNodeTableSources[0].
        numLevels = groupByColumns.size() + 1;
        this.rollupKeyFilters = rollupKeyFilters;
        constituentDepth = numLevels + 1;
        Require.eq(numLevels, "level count", levelTables.length, "levelTables.length");
        this.levelTables = levelTables;
        Require.eq(numLevels, "level count", levelRowLookups.length, "levelRowLookups.length");
        this.levelRowLookups = levelRowLookups;
        Require.eq(numLevels, "level count", levelNodeTableSources.length, "levelNodeTableSources.length");
        this.levelNodeTableSources = levelNodeTableSources;

        this.aggregatedNodeDefinition = aggregatedNodeDefinition == null
                ? computeAggregatedNodeDefinition(getRoot(), aggregatedNodeOperations)
                : aggregatedNodeDefinition;
        this.aggregatedNodeOperations = aggregatedNodeOperations;
        if (includesConstituents) {
            this.constituentNodeDefinition = constituentNodeDefinition == null
                    ? computeConstituentNodeDefinition(source, constituentNodeOperations)
                    : constituentNodeDefinition;
            this.constituentNodeOperations = constituentNodeOperations;
        } else {
            Assert.eqNull(constituentNodeDefinition, "constituentNodeDefinition");
            this.constituentNodeDefinition = null;
            Assert.eqNull(constituentNodeOperations, "constituentNodeOperations");
            this.constituentNodeOperations = null;
        }
        this.availableColumnDefinitions = availableColumnDefinitions == null
                ? computeAvailableColumnDefinitions(this.aggregatedNodeDefinition, this.constituentNodeDefinition)
                : availableColumnDefinitions;

        if (source.isRefreshing()) {
            final Table root = getRoot(); // This is our 0th level aggregation result
            Assert.assertion(root.isRefreshing(), "root.isRefreshing() if source.isRefreshing()");
            // The 0th level aggregation result depends directly or indirectly on all the other aggregation results,
            // their raw constituents, and the source.
            manage(root);
        }
    }

    @Override
    public Collection<? extends Aggregation> getAggregations() {
        return aggregations;
    }

    @Override
    public boolean includesConstituents() {
        return includesConstituents;
    }

    @Override
    public Collection<? extends ColumnName> getGroupByColumns() {
        return groupByColumns;
    }

    @Override
    public String getDescription() {
        return String.format("RollupTable(%s, %s)", source.getDescription(), Strings.ofColumnNames(groupByColumns));
    }

    @Override
    public Table getEmptyExpansionsTable() {
        final Collection<ColumnDefinition<?>> groupByColumnDefinitions = getGroupByColumns().stream()
                .map(gbcn -> getSource().getDefinition().getColumn(gbcn.name()))
                .collect(Collectors.toList());
        final Collection<ColumnDefinition<?>> keyTableColumnDefinitions = Stream.concat(
                Stream.of(ROW_DEPTH_COLUMN_DEFINITION),
                groupByColumnDefinitions.stream())
                .collect(Collectors.toList());
        final TableDefinition keyTableDefinition = TableDefinition.of(keyTableColumnDefinitions);
        // noinspection resource
        return new QueryTable(keyTableDefinition, RowSetFactory.empty().toTracking(),
                NullValueColumnSource.createColumnSourceMap(keyTableDefinition), null, null);
    }

    @Override
    public TableDefinition getNodeDefinition(@NotNull final NodeType nodeType) {
        switch (nodeType) {
            case Aggregated:
                return aggregatedNodeDefinition;
            case Constituent:
                assertIncludesConstituents();
                return constituentNodeDefinition;
            default:
                return unexpectedNodeType(nodeType);
        }
    }

    @Override
    public List<ColumnDefinition<?>> getAvailableColumnDefinitions() {
        return availableColumnDefinitions;
    }

    private static TableDefinition computeAggregatedNodeDefinition(
            @NotNull final Table root,
            @Nullable final RollupNodeOperationsRecorder operations) {
        return operations == null
                ? TableDefinition.of(filterRollupInternalColumns(
                        root.getDefinition().getColumnStream()).collect(Collectors.toList()))
                : operations.getResultDefinition();
    }

    private static TableDefinition computeConstituentNodeDefinition(
            @NotNull final Table source,
            @Nullable final RollupNodeOperationsRecorder operations) {
        return operations == null
                ? source.getDefinition()
                : operations.getResultDefinition();
    }

    private static List<ColumnDefinition<?>> computeAvailableColumnDefinitions(
            @NotNull final TableDefinition aggregatedNodeDefinition,
            @Nullable final TableDefinition constituentNodeDefinition) {
        return Stream.of(
                STRUCTURAL_COLUMN_DEFINITIONS.stream(),
                aggregatedNodeDefinition.getColumnStream(),
                constituentNodeDefinition == null
                        ? Stream.<ColumnDefinition<?>>empty()
                        : constituentNodeDefinition.getColumnStream())
                .flatMap(Function.identity())
                .collect(Collectors.toList());
    }

    @Override
    public RollupTable withFilter(@NotNull final Filter filter) {
        final WhereFilter[] filters = WhereFilter.fromInternal(filter);
        if (filters.length == 0) {
            return noopResult();
        }
        final WhereFilter[] whereFilters = initializeAndValidateFilters(source, aggregations, Arrays.asList(filters),
                IllegalArgumentException::new);

        List<String> groupByColNames = groupByColumns.stream().map(ColumnName::name).collect(Collectors.toList());
        final boolean onlyGroupBy = Arrays.stream(whereFilters)
                .allMatch(w -> groupByColNames.containsAll(w.getColumns()));
        if (!onlyGroupBy) {
            final Table filteredTable = source.where(Filter.and(whereFilters));
            return rebase(filteredTable);
        }

        final QueryTable filteredBaseLevel = (QueryTable) levelTables[numLevels - 1].where(Filter.and(whereFilters));
        final AggregationRowLookup baseLevelRowLookup = levelRowLookups[numLevels - 1];
        final RowSet filteredBaseLevelRowSet = filteredBaseLevel.getRowSet();
        final AggregationRowLookup filteredBaseLevelRowLookup = nodeKey -> {
            final int unfilteredRowKey = baseLevelRowLookup.get(nodeKey);
            // NB: Rollup snapshot patterns allow us to safely always use current, here.
            if (filteredBaseLevelRowSet.find(unfilteredRowKey) >= 0) {
                return unfilteredRowKey;
            }
            return DEFAULT_UNKNOWN_ROW;
        };
        final QueryTable[] levelTables = makeLevelTablesArray(numLevels, filteredBaseLevel);
        final AggregationRowLookup[] levelRowLookups = makeLevelRowLookupsArray(numLevels, filteredBaseLevelRowLookup);
        final ColumnSource<Table>[] levelNodeTableSources = makeLevelNodeTableSourcesArray(
                numLevels, filteredBaseLevel.getColumnSource(ROLLUP_COLUMN.name(), Table.class));
        rollupFromBase(levelTables, levelRowLookups, levelNodeTableSources, aggregations, groupByColumns, source);

        final WhereFilter[] newFilters;
        if (rollupKeyFilters == null) {
            newFilters = whereFilters;
        } else {
            newFilters = Arrays.copyOf(rollupKeyFilters, rollupKeyFilters.length + whereFilters.length);
            System.arraycopy(whereFilters, 0, newFilters, rollupKeyFilters.length, whereFilters.length);
        }

        return new RollupTableImpl(getAttributes(), source, aggregations, includesConstituents, groupByColumns,
                levelTables, levelRowLookups, levelNodeTableSources,
                aggregatedNodeDefinition, aggregatedNodeOperations,
                constituentNodeDefinition, constituentNodeOperations,
                newFilters, availableColumnDefinitions);
    }

    @Override
    public RollupTable withUpdateView(@NotNull final String... columns) {
        return withUpdateView(Selectable.from(columns));
    }

    @Override
    public RollupTable withUpdateView(Collection<Selectable> columns) {
        return withNodeOperations(makeNodeOperationsRecorder(NodeType.Aggregated).updateView(columns));
    }

    /**
     * Initialize and validate the supplied filters for this RollupTable.
     *
     * @param source The rollup {@link #getSource() source}
     * @param aggregations The rollup aggregations}
     * @param filters The filters to initialize and validate
     * @param exceptionFactory A factory for creating exceptions from their messages
     * @return The initialized and validated filters
     */
    public static WhereFilter[] initializeAndValidateFilters(
            @NotNull final Table source,
            @NotNull final Collection<? extends Aggregation> aggregations,
            @NotNull final Collection<? extends Filter> filters,
            @NotNull final Function<String, ? extends RuntimeException> exceptionFactory) {

        final List<String> aggColumns = AggregationPairs.of(aggregations)
                .map(p -> p.input().name()).collect(Collectors.toList());
        final List<String> validColumns = source.getColumnSourceMap().keySet().stream()
                .filter(cn -> !aggColumns.contains(cn)).collect(Collectors.toList());
        final WhereFilter[] whereFilters = WhereFilter.from(filters);
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        for (final WhereFilter whereFilter : whereFilters) {
            whereFilter.init(source.getDefinition(), compilationProcessor);
            final List<String> invalidColumnsUsed = whereFilter.getColumns().stream()
                    .filter(cn -> !validColumns.contains(cn)).collect(Collectors.toList());
            if (!invalidColumnsUsed.isEmpty()) {
                throw exceptionFactory.apply(
                        "Invalid filter found: " + whereFilter + " may only use non-aggregation columns, which are "
                                + validColumns + ", but has used " + invalidColumnsUsed);
            }
            final boolean usesArrays = !whereFilter.getColumnArrays().isEmpty();
            if (usesArrays) {
                throw exceptionFactory.apply("Invalid filter found: " + whereFilter
                        + " may not use column arrays, but uses column arrays from " + whereFilter.getColumnArrays());
            }
        }
        compilationProcessor.compile();

        return whereFilters;
    }

    @Override
    public RollupNodeOperationsRecorder makeNodeOperationsRecorder(@NotNull final NodeType nodeType) {
        return new RollupNodeOperationsRecorder(getNodeDefinition(nodeType), nodeType);
    }

    @Override
    public RollupTable withNodeOperations(@NotNull final NodeOperationsRecorder... nodeOperations) {
        if (Stream.of(nodeOperations).allMatch(no -> no == null || no.isEmpty())) {
            return noopResult();
        }
        List<ColumnDefinition<?>> newAvailableColumnDefinitions = availableColumnDefinitions;
        RollupNodeOperationsRecorder newAggregatedNodeOperations = aggregatedNodeOperations;
        RollupNodeOperationsRecorder newConstituentNodeOperations = constituentNodeOperations;
        for (final NodeOperationsRecorder recorder : nodeOperations) {
            if (recorder == null || recorder.isEmpty()) {
                continue;
            }
            final RollupNodeOperationsRecorder recorderTyped = (RollupNodeOperationsRecorder) recorder;
            if (!recorderTyped.getRecordedFormats().isEmpty() || !recorderTyped.getRecordedUpdateViews().isEmpty()) {
                // Need to rebuild the available column definitions.
                newAvailableColumnDefinitions = null;
            }
            switch (recorderTyped.getNodeType()) {
                case Aggregated:
                    newAggregatedNodeOperations = accumulateOperations(newAggregatedNodeOperations, recorderTyped);
                    break;
                case Constituent:
                    assertIncludesConstituents();
                    newConstituentNodeOperations = accumulateOperations(newConstituentNodeOperations, recorderTyped);
                    break;
                default:
                    return unexpectedNodeType(recorderTyped.getNodeType());
            }
        }
        return new RollupTableImpl(getAttributes(), source, aggregations, includesConstituents, groupByColumns,
                levelTables, levelRowLookups, levelNodeTableSources,
                null, newAggregatedNodeOperations, null, newConstituentNodeOperations,
                rollupKeyFilters, newAvailableColumnDefinitions);
    }

    @Override
    public NodeOperationsRecorder translateAggregatedNodeOperationsForConstituentNodes(
            @NotNull final NodeOperationsRecorder aggregatedNodeOperationsToTranslate) {
        final RollupNodeOperationsRecorder aggregated =
                (RollupNodeOperationsRecorder) aggregatedNodeOperationsToTranslate;
        RollupNodeOperationsRecorder constituent = makeNodeOperationsRecorder(NodeType.Constituent);
        final TableDefinition aggregatedTableDefinition = getNodeDefinition(NodeType.Aggregated);
        final TableDefinition constituentTableDefinition = getNodeDefinition(NodeType.Constituent);

        constituent = translateFormats(aggregated, constituent, aggregatedTableDefinition, constituentTableDefinition);
        constituent =
                translateUpdateViews(aggregated, constituent, aggregatedTableDefinition, constituentTableDefinition);

        final Map<String, String> aggregatedConstituentPairs = AggregationPairs.of(aggregations)
                .collect(Collectors.toMap(p -> p.output().name(), p -> p.input().name()));
        final Set<String> groupByColumnNames =
                groupByColumns.stream().map(ColumnName::name).collect(Collectors.toSet());
        return translateSorts(aggregated, constituent,
                constituentTableDefinition, groupByColumnNames, aggregatedConstituentPairs);
    }

    private static RollupNodeOperationsRecorder translateFormats(
            @NotNull final RollupNodeOperationsRecorder aggregated,
            @NotNull final RollupNodeOperationsRecorder constituent,
            @NotNull final TableDefinition aggregatedTableDefinition,
            @NotNull final TableDefinition constituentTableDefinition) {
        if (aggregated.getRecordedFormats().isEmpty()) {
            return constituent;
        }
        final List<? extends SelectColumn> constituentFormats = aggregated.getRecordedFormats().stream()
                .filter((final SelectColumn aggregatedFormat) -> Stream
                        .concat(aggregatedFormat.getColumns().stream(), aggregatedFormat.getColumnArrays().stream())
                        .allMatch((final String columnName) -> {
                            final ColumnDefinition<?> constituentColumnDefinition =
                                    constituentTableDefinition.getColumn(columnName);
                            if (constituentColumnDefinition == null) {
                                return false;
                            }
                            final ColumnDefinition<?> aggregatedColumnDefinition =
                                    aggregatedTableDefinition.getColumn(columnName);
                            return constituentColumnDefinition.isCompatible(aggregatedColumnDefinition);
                        }))
                .collect(Collectors.toList());
        if (constituentFormats.isEmpty()) {
            return constituent;
        }
        return (RollupNodeOperationsRecorder) constituent.withFormats(constituentFormats.stream());
    }

    private static RollupNodeOperationsRecorder translateUpdateViews(
            @NotNull final RollupNodeOperationsRecorder aggregated,
            @NotNull final RollupNodeOperationsRecorder constituent,
            @NotNull final TableDefinition aggregatedTableDefinition,
            @NotNull final TableDefinition constituentTableDefinition) {
        if (aggregated.getRecordedUpdateViews().isEmpty()) {
            return constituent;
        }
        final List<Selectable> constituentViews = aggregated.getRecordedUpdateViews().stream()
                .filter((final SelectColumn aggregatedView) -> Stream
                        .concat(aggregatedView.getColumns().stream(), aggregatedView.getColumnArrays().stream())
                        .allMatch((final String columnName) -> {
                            final ColumnDefinition<?> constituentColumnDefinition =
                                    constituentTableDefinition.getColumn(columnName);
                            if (constituentColumnDefinition == null) {
                                return false;
                            }
                            final ColumnDefinition<?> aggregatedColumnDefinition =
                                    aggregatedTableDefinition.getColumn(columnName);
                            return constituentColumnDefinition.isCompatible(aggregatedColumnDefinition);
                        }))
                .collect(Collectors.toList());
        if (constituentViews.isEmpty()) {
            return constituent;
        }
        return (RollupNodeOperationsRecorder) constituent.updateView(constituentViews);
    }

    private static RollupNodeOperationsRecorder translateSorts(
            @NotNull final RollupNodeOperationsRecorder aggregated,
            @NotNull final RollupNodeOperationsRecorder constituent,
            @NotNull final TableDefinition constituentTableDefinition,
            @NotNull final Set<String> groupByColumnNames,
            @NotNull final Map<String, String> aggregatedConstituentPairs) {
        if (aggregated.getRecordedSorts().isEmpty()) {
            return constituent;
        }
        final List<SortColumn> constituentSortColumns = aggregated.getRecordedSorts().stream()
                .map((final SortColumn aggregatedSortColumn) -> {
                    String aggregatedColumnName = aggregatedSortColumn.column().name();
                    final boolean aggregatedAbsolute = isAbsoluteColumnName(aggregatedColumnName);
                    if (aggregatedAbsolute) {
                        aggregatedColumnName = absoluteColumnNameToBaseName(aggregatedColumnName);
                    }
                    if (groupByColumnNames.contains(aggregatedColumnName)) {
                        // All constituents with the same parent node share the same values for all group-by columns,
                        // so there's never a need to apply any sort for those columns.
                        return null;
                    }
                    String constituentColumnName = aggregatedConstituentPairs.get(aggregatedColumnName);
                    if (constituentColumnName == null) {
                        // Some aggregations have no input, and hence there is no corresponding constituent column to
                        // sort.
                        return null;
                    }
                    if (aggregatedAbsolute) {
                        final ColumnDefinition<?> constituentColumnDefinition =
                                constituentTableDefinition.getColumn(constituentColumnName);
                        if (Number.class.isAssignableFrom(
                                TypeUtils.getBoxedType(constituentColumnDefinition.getDataType()))) {
                            constituentColumnName = baseColumnNameToAbsoluteName(constituentColumnName);
                        }
                    }
                    return aggregatedSortColumn.isAscending()
                            ? SortColumn.asc(ColumnName.of(constituentColumnName))
                            : SortColumn.desc(ColumnName.of(constituentColumnName));

                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (constituentSortColumns.isEmpty()) {
            return constituent;
        }
        // Let the logic in SortRecordingTableAdapter handle absolute views and column repetition filtering.
        return (RollupNodeOperationsRecorder) constituent.sort(constituentSortColumns);
    }

    private static RollupNodeOperationsRecorder accumulateOperations(
            @Nullable final RollupNodeOperationsRecorder existing,
            @NotNull final RollupNodeOperationsRecorder added) {
        return existing == null ? added : existing.withOperations(added);
    }

    private void assertIncludesConstituents() {
        if (getLeafNodeType() != NodeType.Constituent) {
            throw new IllegalArgumentException("Rollup does not have constituent nodes");
        }
    }

    private static <T> T unexpectedNodeType(@NotNull final NodeType nodeType) {
        throw new IllegalArgumentException("Unrecognized node type: " + nodeType);
    }

    @Override
    protected RollupTableImpl copy() {
        return new RollupTableImpl(getAttributes(), source, aggregations, includesConstituents, groupByColumns,
                levelTables, levelRowLookups, levelNodeTableSources,
                aggregatedNodeDefinition, aggregatedNodeOperations,
                constituentNodeDefinition, constituentNodeOperations,
                rollupKeyFilters, availableColumnDefinitions);
    }

    @Override
    public RollupTableImpl rebase(@NotNull final Table newSource) {
        checkRebaseDefinition("RollupTable", source, newSource);

        final QueryTable newSourceQueryTable;
        if (rollupKeyFilters != null) {
            // if this rollup has had any key filters, then we apply them to the new source rather than generating
            // buckets for keys which cannot be present in the result. Because these filters are applied, we pass
            // in a null set of filters to the new RollupTableImpl we are creating.
            final List<WhereFilter> keyFilters =
                    Arrays.stream(rollupKeyFilters).map(WhereFilter::copy).collect(Collectors.toList());
            newSourceQueryTable = (QueryTable) newSource.where(Filter.and(keyFilters)).coalesce();
        } else {
            newSourceQueryTable = (QueryTable) newSource.coalesce();
        }

        return makeRollupInternal(
                getAttributes(),
                newSourceQueryTable, aggregations, includesConstituents, groupByColumns,
                aggregatedNodeDefinition, aggregatedNodeOperations, constituentNodeDefinition,
                constituentNodeOperations,
                availableColumnDefinitions);
    }

    public static RollupTable makeRollup(
            @NotNull final QueryTable source,
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includeConstituents,
            @NotNull final Collection<? extends ColumnName> groupByColumns) {
        final RollupTableImpl result =
                makeRollupInternal(source.getAttributes(ak -> shouldCopyAttribute(ak, CopyAttributeOperation.Rollup)),
                        source, aggregations, includeConstituents, groupByColumns,
                        null, null, null, null, null);
        final TableDefinition baseDefinition = result.levelTables[groupByColumns.size()].getDefinition();
        source.copySortableColumns(result, baseDefinition.getColumnNameSet()::contains);
        result.setColumnDescriptions(AggregationDescriptions.of(aggregations));
        return result;
    }

    private static @NotNull RollupTableImpl makeRollupInternal(
            @NotNull final Map<String, Object> attributes,
            @NotNull final QueryTable source,
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includeConstituents,
            @NotNull final Collection<? extends ColumnName> groupByColumns,
            final TableDefinition aggregatedNodeDefinition,
            final RollupNodeOperationsRecorder aggregatedNodeOperations,
            final TableDefinition constituentNodeDefinition,
            final RollupNodeOperationsRecorder constituentNodeOperations,
            final List<ColumnDefinition<?>> availableColumnDefinitions) {
        final int numLevels = groupByColumns.size() + 1;
        final QueryTable baseLevel = source.aggNoMemo(
                AggregationProcessor.forRollupBase(aggregations, includeConstituents, ROLLUP_COLUMN),
                false, null, groupByColumns);
        final QueryTable[] levelTables = makeLevelTablesArray(numLevels, baseLevel);
        final AggregationRowLookup[] levelRowLookups = makeLevelRowLookupsArray(numLevels, getRowLookup(baseLevel));
        final ColumnSource<Table>[] levelNodeTableSources = makeLevelNodeTableSourcesArray(
                numLevels, baseLevel.getColumnSource(ROLLUP_COLUMN.name(), Table.class));
        rollupFromBase(levelTables, levelRowLookups, levelNodeTableSources, aggregations, groupByColumns, source);

        return new RollupTableImpl(
                attributes,
                source, aggregations, includeConstituents, groupByColumns,
                levelTables, levelRowLookups, levelNodeTableSources, aggregatedNodeDefinition, aggregatedNodeOperations,
                constituentNodeDefinition, constituentNodeOperations, null, availableColumnDefinitions);
    }


    /**
     * <p>
     * Create a RollupTable from internal data structures that have been made outside the rollup code.
     * </p>
     *
     * <p>
     * Constituents may not be included, and node operations are not provided.
     * </p>
     *
     * <p>
     * Note: This function is not part of the public Deephaven API, and the semantics are subject to change at any time.
     * </p>
     *
     * @param source the original table being aggregated for this rollup
     * @param attributes the attributes of the result rollup
     * @param aggregations the aggregations used to make this rollup
     * @param groupByColumns the group by columns for this rollup
     * @param levelTables an array of tables for each level
     * @param levelRowLookups an array of row lookup structures for each level
     * @param levelNodeTableSources an array of column sources containing a lower-level table
     * @return a RollupTable from the provided data structures
     */
    @InternalUseOnly
    public static RollupTable makeFromPartsInternal(
            @NotNull final QueryTable source,
            @NotNull final Map<String, Object> attributes,
            @NotNull final Collection<? extends Aggregation> aggregations,
            @NotNull final Collection<? extends ColumnName> groupByColumns,
            @NotNull final QueryTable[] levelTables,
            @NotNull final AggregationRowLookup[] levelRowLookups,
            @NotNull final ColumnSource<Table>[] levelNodeTableSources) {
        return new RollupTableImpl(
                attributes,
                source, aggregations, false, groupByColumns,
                levelTables, levelRowLookups, levelNodeTableSources,
                null, null, null, null, null, null);
    }

    private static QueryTable[] makeLevelTablesArray(
            final int numLevels, @NotNull final QueryTable baseLevelTable) {
        final QueryTable[] levelTables = new QueryTable[numLevels];
        levelTables[numLevels - 1] = baseLevelTable;
        return levelTables;
    }

    private static AggregationRowLookup[] makeLevelRowLookupsArray(
            final int numLevels,
            @NotNull final AggregationRowLookup baseLevelRowLookup) {
        final AggregationRowLookup[] levelRowLookups = new AggregationRowLookup[numLevels];
        levelRowLookups[numLevels - 1] = baseLevelRowLookup;
        return levelRowLookups;
    }

    private static ColumnSource<Table>[] makeLevelNodeTableSourcesArray(
            final int numLevels,
            @NotNull final ColumnSource<Table> baseLevelNodeTableSource) {
        // noinspection unchecked
        final ColumnSource<Table>[] levelNodeTableSources = new ColumnSource[numLevels];
        levelNodeTableSources[numLevels - 1] = baseLevelNodeTableSource;
        return levelNodeTableSources;
    }

    /**
     * Reaggregate the base level and fill in the level arrays for a rollup.
     *
     * @param levelTables Input/output array for per-level tables, with the base level already filled
     * @param levelRowLookups Input/output array for per-level row lookups, with the base level already filled
     * @param levelNodeTableSources Input/output array for per-level node table column sources, with the base level
     *        already filled
     * @param aggregations The aggregations
     * @param groupByColumns The group-by columns
     * @param source the source table for the rollup
     */
    private static void rollupFromBase(
            @NotNull final QueryTable[] levelTables,
            @NotNull final AggregationRowLookup[] levelRowLookups,
            @NotNull final ColumnSource<Table>[] levelNodeTableSources,
            @NotNull final Collection<? extends Aggregation> aggregations,
            @NotNull final Collection<? extends ColumnName> groupByColumns,
            @NotNull final QueryTable source) {
        final Deque<ColumnName> columnsToReaggregateBy = new ArrayDeque<>(groupByColumns);
        final Deque<String> nullColumnNames = new ArrayDeque<>(groupByColumns.size());
        int lastLevelIndex = levelTables.length - 1;
        QueryTable lastLevel = levelTables[lastLevelIndex];
        while (!columnsToReaggregateBy.isEmpty()) {
            nullColumnNames.addFirst(columnsToReaggregateBy.removeLast().name());
            final TableDefinition lastLevelDefinition = lastLevel.getDefinition();
            final List<ColumnDefinition<?>> nullColumns =
                    nullColumnNames.stream().map(lastLevelDefinition::getColumn).collect(Collectors.toList());

            lastLevel = lastLevel.aggNoMemo(
                    AggregationProcessor.forRollupReaggregated(aggregations, nullColumns, ROLLUP_COLUMN, source),
                    false, null, new ArrayList<>(columnsToReaggregateBy));
            --lastLevelIndex;
            levelTables[lastLevelIndex] = lastLevel;
            levelRowLookups[lastLevelIndex] = getRowLookup(lastLevel);
            levelNodeTableSources[lastLevelIndex] = lastLevel.getColumnSource(ROLLUP_COLUMN.name(), Table.class);
        }
        Assert.eqZero(lastLevelIndex, "lastLevelIndex");
    }

    private static Stream<ColumnDefinition<?>> filterRollupInternalColumns(
            @NotNull final Stream<ColumnDefinition<?>> columnDefinitions) {
        return columnDefinitions.filter(cd -> !cd.getName().endsWith(ROLLUP_COLUMN_SUFFIX)
                && !cd.getName().equals(EXPOSED_GROUP_ROW_SETS.name()));
    }

    @Override
    Iterable<Object> getDefaultExpansionNodeKeys() {
        if (groupByColumns.isEmpty() && !includesConstituents) {
            return Collections.singletonList(ROOT_NODE_KEY);
        } else {
            return List.of(ROOT_NODE_KEY, EMPTY_KEY);
        }
    }

    @Override
    ChunkSource.WithPrev<? extends Values> makeNodeKeySource(@NotNull final Table nodeKeyTable) {
        return new RollupNodeKeySource(
                nodeKeyTable.getColumnSource(getRowDepthColumn().name(), int.class),
                groupByColumns.stream()
                        .map(cn -> nodeKeyTable.getColumnSource(cn.name(),
                                getRoot().getColumnSource(cn.name()).getType()))
                        .toArray(ColumnSource[]::new));
    }

    private static int nodeDepth(@Nullable final Object nodeKey) {
        if (nodeKey == ROOT_NODE_KEY) {
            return ROOT_NODE_DEPTH;
        }
        final int nodeKeyWidth = nodeKey instanceof Object[] ? ((Object[]) nodeKey).length : 1;
        return nodeKeyWidth + 1;
    }

    private static int nodeDepth(final long nodeId) {
        return (int) (nodeId >>> 32);
    }

    private static int nodeSlot(final long nodeId) {
        return (int) nodeId;
    }

    private static long makeNodeId(final int nodeDepth, final int nodeSlot) {
        // NB: nodeDepth is an integer in [ROOT_PARENT_NODE_DEPTH, baseLevelParentDepth], and nodeSlot is an
        // integer in [0, 1 << 30)
        return ((long) nodeDepth << 32) | nodeSlot;
    }

    @Override
    boolean isRootNodeKey(@Nullable final Object nodeKey) {
        return nodeKey == ROOT_NODE_KEY;
    }

    @Override
    long nodeKeyToNodeId(@Nullable final Object nodeKey) {
        if (nodeKey == ROOT_NODE_KEY) {
            return ROOT_NODE_ID;
        }
        final int nodeDepth = nodeDepth(nodeKey);
        if (nodeDepth < 0 || nodeDepth > numLevels) {
            return NULL_NODE_ID;
        }
        if (nodeDepth == numLevels && !includesConstituents) {
            return NULL_NODE_ID;
        }

        final AggregationRowLookup rowLookup = levelRowLookups[nodeDepth - 1];
        final int nodeSlot = rowLookup.get(nodeKey);
        if (nodeSlot == rowLookup.noEntryValue()) {
            return NULL_NODE_ID;
        }

        return makeNodeId(nodeDepth, nodeSlot);
    }

    @Override
    long nullNodeId() {
        return NULL_NODE_ID;
    }

    @Override
    long rootNodeId() {
        return ROOT_NODE_ID;
    }

    @Override
    long findRowKeyInParentUnsorted(
            final long childNodeId,
            @Nullable final Object childNodeKey,
            final boolean usePrev) {
        if (childNodeId == NULL_NODE_ID) {
            return NULL_ROW_KEY;
        }

        final int nodeDepth = nodeDepth(childNodeId);
        final int nodeSlot = nodeSlot(childNodeId);

        final TrackingRowSet rowSet = levelTables[nodeDepth - 1].getRowSet();
        if ((usePrev ? rowSet.findPrev(nodeSlot) : rowSet.find(nodeSlot)) == NULL_ROW_KEY) {
            // the aggregation knows about this key, but it does not actually exist in the table
            return NULL_ROW_KEY;
        }

        return nodeSlot;
    }

    @Override
    @Nullable
    Boolean findParentNodeKey(
            @Nullable final Object childNodeKey,
            final long childRowKeyInParentUnsorted,
            final boolean usePrev,
            @NotNull final MutableObject<Object> parentNodeKeyHolder) {
        final int nodeDepth = nodeDepth(childNodeKey);
        if (nodeDepth > numLevels) {
            throw new IllegalArgumentException("Invalid node key " + Arrays.toString((Object[]) childNodeKey)
                    + ": deeper than maximum " + numLevels);
        }
        switch (nodeDepth) {
            case ROOT_NODE_DEPTH:
                return null;
            case 1:
                parentNodeKeyHolder.setValue(ROOT_NODE_KEY);
                return true;
            case 2:
                parentNodeKeyHolder.setValue(EMPTY_KEY);
                return true;
            case 3:
                // noinspection ConstantConditions (null falls under "case 2")
                parentNodeKeyHolder.setValue(((Object[]) childNodeKey)[0]);
                return true;
            default:
                // noinspection ConstantConditions (null falls under "case 2")
                parentNodeKeyHolder.setValue(Arrays.copyOf(((Object[]) childNodeKey), nodeDepth - 2));
                return true;
        }
    }

    @Override
    @Nullable
    Table nodeIdToNodeBaseTable(final long nodeId) {
        if (nodeId == ROOT_NODE_ID) {
            return getRoot();
        }
        final int nodeDepth = nodeDepth(nodeId);
        if (nodeDepth < 0 || nodeDepth > numLevels) {
            return null;
        }
        if (nodeDepth < numLevels || includesConstituents) {
            final int nodeSlot = nodeSlot(nodeId);
            return levelNodeTableSources[nodeDepth - 1].get(nodeSlot);
        }
        return null;
    }

    private RollupNodeOperationsRecorder nodeOperations(final long nodeId) {
        final int nodeDepth = nodeDepth(nodeId);
        if (nodeDepth == numLevels) {
            // We must be including constituents
            return constituentNodeOperations;
        }
        return aggregatedNodeOperations;
    }

    @Override
    boolean hasNodeFiltersToApply(final long nodeId) {
        return false;
    }

    @Override
    Table applyNodeFormatsAndFilters(final long nodeId, @NotNull final Table nodeBaseTable) {
        Table result = RollupNodeOperationsRecorder.applyUpdateViews(nodeOperations(nodeId), nodeBaseTable);
        return BaseNodeOperationsRecorder.applyFormats(nodeOperations(nodeId), result);
        // NB: There is no node-level filtering for rollups
    }

    @Override
    Table applyNodeSorts(final long nodeId, @NotNull final Table nodeFilteredTable) {
        return BaseNodeOperationsRecorder.applySorts(nodeOperations(nodeId), nodeFilteredTable);
    }

    @Override
    @NotNull
    ChunkSource.WithPrev<? extends Values>[] makeOrFillChunkSourceArray(
            @NotNull final SnapshotStateImpl snapshotState,
            final long nodeId,
            @NotNull final Table nodeSortedTable,
            @Nullable final ChunkSource.WithPrev<? extends Values>[] existingChunkSources) {
        // @formatter:off
        // For all nodes, we have a prefix of:
        //   - "row depth" -> int, how deep is this node in the rollup? (also how wide (in columns) is the group-by key
        //     for this row?)
        //   - "row expanded" -> Boolean, always handled by the parent class, ignored here
        // For nodes above the base level, we continue with:
        //   - All columns from the aggregated table definition, which includes formatting columns but not the column of
        //       tables or any absolute views for sorting
        //   - Null-value sources for all constituent columns
        // For base level nodes (only expandable when constituents are included), we continue with:
        //   - Null-value sources for all aggregated columns
        //   - All columns from the constituent table definition, which includes formatting columns but not any absolute
        //       views for sorting
        // @formatter:on
        final int firstConstituentColumnIndex = FIRST_AGGREGATED_COLUMN_INDEX + aggregatedNodeDefinition.numColumns();
        final int numColumns = firstConstituentColumnIndex
                + (includesConstituents ? constituentNodeDefinition.numColumns() : 0);
        final ChunkSource.WithPrev<? extends Values>[] result =
                maybeAllocateResultChunkSourceArray(existingChunkSources, numColumns);

        final int nodeDepth = nodeDepth(nodeId);
        Assert.eq(nodeDepth + 1, "nodeDepth + 1", snapshotState.getCurrentDepth(), "current snapshot traversal depth");
        final boolean isBaseLevel = nodeDepth == numLevels;
        if (isBaseLevel) {
            Assert.assertion(includesConstituents, "includesConstituents",
                    "filling from base level when not including constituents");
        }

        final BitSet columns = snapshotState.getColumns();
        for (int ci = columns.nextSetBit(0); ci >= 0; ci = columns.nextSetBit(ci + 1)) {
            if (result[ci] != null || ci == ROW_EXPANDED_COLUMN_INDEX) {
                continue;
            }
            if (ci == ROW_DEPTH_COLUMN_INDEX) {
                // Depth is the depth for the child rows to be filled from, not actually this node
                result[ci] = getDepthSource(nodeDepth + 1);
            } else if (ci < firstConstituentColumnIndex) {
                final ColumnDefinition<?> cd =
                        aggregatedNodeDefinition.getColumns().get(ci - FIRST_AGGREGATED_COLUMN_INDEX);
                result[ci] = isBaseLevel
                        ? NullValueColumnSource.getInstance(cd.getDataType(), cd.getComponentType())
                        : maybeConvertToPrimitive(nodeSortedTable.getColumnSource(cd.getName(), cd.getDataType()));
            } else {
                final ColumnDefinition<?> cd =
                        constituentNodeDefinition.getColumns().get(ci - firstConstituentColumnIndex);
                result[ci] = isBaseLevel
                        ? maybeConvertToPrimitive(nodeSortedTable.getColumnSource(cd.getName(), cd.getDataType()))
                        : NullValueColumnSource.getInstance(cd.getDataType(), cd.getComponentType());
            }
        }
        return result;
    }

    @Override
    LevelExpandable levelExpandable(@NotNull final SnapshotStateImpl snapshotState) {
        final int levelDepth = snapshotState.getCurrentDepth();

        Assert.leq(levelDepth, "levelDepth",
                includesConstituents ? constituentDepth : numLevels,
                "includesConstituents ? constituentDepth : numLevels");

        return levelDepth < numLevels || (levelDepth == numLevels && includesConstituents)
                ? All
                : None;
    }

    @Override
    @NotNull
    LongUnaryOperator makeChildNodeIdLookup(
            @NotNull final SnapshotStateImpl snapshotState,
            @NotNull final Table nodeTableToExpand,
            final boolean sorted) {
        final int levelDepth = snapshotState.getCurrentDepth();

        Assert.leq(levelDepth, "levelDepth",
                includesConstituents ? constituentDepth : numLevels,
                "includesConstituents ? constituentDepth : numLevels");

        if (levelDepth < numLevels || (levelDepth == numLevels && includesConstituents)) {
            final RowRedirection sortRedirection = sorted ? SortOperation.getRowRedirection(nodeTableToExpand) : null;
            if (sortRedirection == null) {
                return (final long rowKey) -> makeNodeId(levelDepth, (int) rowKey);
            }
            return snapshotState.usePrev()
                    ? (final long sortedRowKey) -> makeNodeId(levelDepth, (int) sortRedirection.getPrev(sortedRowKey))
                    : (final long sortedRowKey) -> makeNodeId(levelDepth, (int) sortRedirection.get(sortedRowKey));
        }

        // There are no expandable nodes below the base level ever.
        // Base level nodes are only expandable if we're including constituents.
        return (final long rowKey) -> NULL_NODE_ID;
    }

    @Override
    boolean nodeIdExpandable(@NotNull final SnapshotStateImpl snapshotState, final long nodeId) {
        return nodeId != NULL_NODE_ID;
    }

    @Override
    NotificationStepSource[] getSourceDependencies() {
        return new NotificationStepSource[] {source};
    }

    @Override
    void maybeWaitForStructuralSatisfaction() {
        maybeWaitForSatisfaction(getRoot());
    }
}
