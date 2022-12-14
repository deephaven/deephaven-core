package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationDescriptions;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.BaseTable.CopyAttributeOperation;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.api.ColumnName.names;
import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.BaseTable.shouldCopyAttribute;
import static io.deephaven.engine.table.impl.by.AggregationRowLookup.DEFAULT_UNKNOWN_ROW;
import static io.deephaven.engine.table.impl.by.AggregationProcessor.getRowLookup;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_COLUMN_SUFFIX;

/**
 * {@link RollupTable} implementation.
 */
public class RollupTableImpl extends HierarchicalTableImpl<RollupTable, RollupTableImpl> implements RollupTable {

    private static final int FIRST_AGGREGATED_COLUMN_INDEX = EXTRA_COLUMN_COUNT;
    private static final ColumnName ROLLUP_COLUMN = ColumnName.of(ROLLUP_COLUMN_SUFFIX);

    private final Collection<? extends Aggregation> aggregations;
    private final boolean includesConstituents;
    private final Collection<? extends ColumnName> groupByColumns;

    private final int numLevels;
    private final int baseLevelIndex;
    private final QueryTable[] levelTables;
    private final AggregationRowLookup[] levelRowLookups;
    private final ColumnSource<Table>[] levelNodeTableSources;

    private final TableDefinition aggregatedNodeDefinition;
    private final RollupNodeOperationsRecorder aggregatedNodeOperations;
    private final TableDefinition constituentNodeDefinition;
    private final RollupNodeOperationsRecorder constituentNodeOperations;

    private RollupTableImpl(
            @NotNull final Map<String, Object> initialAttributes,
            @NotNull final QueryTable source,
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includesConstituents,
            @NotNull final Collection<? extends ColumnName> groupByColumns,
            @NotNull final QueryTable[] levelTables,
            @NotNull final AggregationRowLookup[] levelRowLookups,
            @Nullable final TableDefinition aggregatedNodeDefinition,
            @Nullable final RollupNodeOperationsRecorder aggregatedNodeOperations,
            @Nullable final TableDefinition constituentNodeDefinition,
            @Nullable final RollupNodeOperationsRecorder constituentNodeOperations) {
        super(initialAttributes, source, levelTables[0]);

        this.aggregations = aggregations;
        this.includesConstituents = includesConstituents;
        this.groupByColumns = groupByColumns;

        numLevels = groupByColumns.size() + 1;
        baseLevelIndex = numLevels - 1;
        Require.eq(numLevels, "level count", levelTables.length, "levelTables.length");
        this.levelTables = levelTables;
        Require.eq(numLevels, "level count", levelRowLookups.length, "levelRowLookups.length");
        this.levelRowLookups = levelRowLookups;
        // noinspection unchecked
        levelNodeTableSources = Stream.of(levelTables)
                .map(lt -> lt.getColumnSource(ROLLUP_COLUMN.name(), Table.class))
                .toArray(ColumnSource[]::new);

        this.aggregatedNodeDefinition = aggregatedNodeDefinition == null
                ? computeAggregatedNodeDefinition(getRoot(), aggregatedNodeOperations)
                : aggregatedNodeDefinition;
        this.aggregatedNodeOperations = aggregatedNodeOperations;
        if (includesConstituents()) {
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

    // TODO-RWC: Use this for schema generation
    // private static TableDefinition computeSnapshotDefinition(
    // @NotNull final TableDefinition aggregatedNodeDefinition,
    // @Nullable final TableDefinition constituentNodeDefinition) {
    // final List<ColumnDefinition<?>> columnDefinitions = new ArrayList<>(
    // EXTRA_COLUMN_COUNT
    // + aggregatedNodeDefinition.numColumns()
    // + (constituentNodeDefinition == null ? 0 : constituentNodeDefinition.numColumns()));
    // columnDefinitions.add(ROW_DEPTH_COLUMN_DEFINITION);
    // columnDefinitions.add(ROW_EXPANDED_COLUMN_DEFINITION);
    // columnDefinitions.addAll(aggregatedNodeDefinition.getColumns());
    // if (constituentNodeDefinition != null) {
    // columnDefinitions.addAll(constituentNodeDefinition.getColumns());
    // }
    // return TableDefinition.of(columnDefinitions);
    // }

    @Override
    public Collection<? extends Pair> getColumnPairs() {
        return AggregationPairs.of(aggregations).collect(Collectors.toList());
    }

    @Override
    public RollupTable withFilters(@NotNull final Collection<? extends Filter> filters) {
        if (filters.isEmpty()) {
            return noopResult();
        }

        final WhereFilter[] whereFilters = initializeAndValidateFilters(filters);
        final QueryTable filteredBaseLevel = (QueryTable) levelTables[baseLevelIndex].where(whereFilters);
        final AggregationRowLookup baseLevelRowLookup = levelRowLookups[baseLevelIndex];
        final RowSet filteredBaseLevelRowSet = filteredBaseLevel.getRowSet();
        final AggregationRowLookup filteredBaseLevelRowLookup = nodeKey -> {
            final int unfilteredRowKey = baseLevelRowLookup.get(nodeKey);
            // NB: Rollup snapshot patterns allow us to safely always use current, here.
            if (filteredBaseLevelRowSet.find(unfilteredRowKey) > 0) {
                return unfilteredRowKey;
            }
            return DEFAULT_UNKNOWN_ROW;
        };
        final QueryTable[] levelTables = makeLevelTablesArray(numLevels, filteredBaseLevel);
        final AggregationRowLookup[] levelRowLookups = makeLevelRowLookupsArray(numLevels, filteredBaseLevelRowLookup);
        rollupFromBase(levelTables, levelRowLookups, aggregations, groupByColumns);
        return new RollupTableImpl(getAttributes(), source, aggregations, includesConstituents, groupByColumns,
                levelTables, levelRowLookups, aggregatedNodeDefinition, aggregatedNodeOperations,
                constituentNodeDefinition, constituentNodeOperations);
    }

    private WhereFilter[] initializeAndValidateFilters(@NotNull final Collection<? extends Filter> filters) {
        final WhereFilter[] whereFilters = WhereFilter.from(filters);
        for (final WhereFilter whereFilter : whereFilters) {
            whereFilter.init(source.getDefinition());
            final List<String> invalidColumnsUsed = whereFilter.getColumns().stream().map(ColumnName::of)
                    .filter(cn -> !groupByColumns.contains(cn)).map(ColumnName::name).collect(Collectors.toList());
            if (!invalidColumnsUsed.isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid filter found: " + whereFilter + " may only use group-by columns, which are "
                                + names(groupByColumns) + ", but has also used " + invalidColumnsUsed);
            }
            final boolean usesArrays = !whereFilter.getColumnArrays().isEmpty();
            if (usesArrays) {
                throw new IllegalArgumentException("Invalid filter found: " + whereFilter
                        + " may not use column arrays, but uses column arrays from " + whereFilter.getColumnArrays());
            }
        }
        return whereFilters;
    }

    @Override
    public NodeOperationsRecorder makeNodeOperationsRecorder(@NotNull final NodeType nodeType) {
        return new RollupNodeOperationsRecorder(getNodeDefinition(nodeType), nodeType);
    }

    @Override
    public RollupTable withNodeOperations(@NotNull final NodeOperationsRecorder... nodeOperations) {
        if (Stream.of(nodeOperations).allMatch(Objects::isNull)) {
            return noopResult();
        }
        RollupNodeOperationsRecorder newAggregatedNodeOperations = aggregatedNodeOperations;
        RollupNodeOperationsRecorder newConstituentNodeOperations = constituentNodeOperations;
        for (final NodeOperationsRecorder recorder : nodeOperations) {
            final RollupNodeOperationsRecorder recorderTyped = (RollupNodeOperationsRecorder) recorder;
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
                levelTables, levelRowLookups, null, newAggregatedNodeOperations, null, newConstituentNodeOperations);
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

    private static <T> T unexpectedNodeType(@NotNull NodeType nodeType) {
        throw new IllegalArgumentException("Unrecognized node type: " + nodeType);
    }

    @Override
    protected RollupTableImpl copy() {
        return new RollupTableImpl(getAttributes(), source, aggregations, includesConstituents, groupByColumns,
                levelTables, levelRowLookups, aggregatedNodeDefinition, aggregatedNodeOperations,
                constituentNodeDefinition, constituentNodeOperations);
    }

    public static RollupTable makeRollup(
            @NotNull final QueryTable source,
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includeConstituents,
            @NotNull final Collection<? extends ColumnName> groupByColumns) {
        final int numLevels = groupByColumns.size() + 1;
        final QueryTable baseLevel = source.aggNoMemo(
                AggregationProcessor.forRollupBase(aggregations, includeConstituents, ROLLUP_COLUMN),
                false, null, groupByColumns);
        final QueryTable[] levelTables = makeLevelTablesArray(numLevels, baseLevel);
        final AggregationRowLookup[] levelRowLookups = makeLevelRowLookupsArray(numLevels, getRowLookup(baseLevel));
        rollupFromBase(levelTables, levelRowLookups, aggregations, groupByColumns);
        final RollupTableImpl result = new RollupTableImpl(
                source.getAttributes(ak -> shouldCopyAttribute(ak, CopyAttributeOperation.Rollup)),
                source, aggregations, includeConstituents, groupByColumns, levelTables, levelRowLookups,
                null, null, null, null);
        source.copySortableColumns(result, baseLevel.getDefinition().getColumnNameMap()::containsKey);
        result.setColumnDescriptions(AggregationDescriptions.of(aggregations));
        return result;
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

    /**
     * Reaggregate the base level and fill in the level arrays for a rollup.
     *
     * @param levelTables Input/output array for per-level tables, with the base level already filled
     * @param levelRowLookups Input/output array for per-level row lookups, with the base level already filled
     * @param aggregations The aggregations
     * @param groupByColumns The group-by columns
     */
    private static void rollupFromBase(
            @NotNull final QueryTable[] levelTables,
            @NotNull final AggregationRowLookup[] levelRowLookups,
            @NotNull final Collection<? extends Aggregation> aggregations,
            @NotNull final Collection<? extends ColumnName> groupByColumns) {
        final Deque<ColumnName> columnsToReaggregateBy = new ArrayDeque<>(groupByColumns);
        final Deque<String> nullColumnNames = new ArrayDeque<>(groupByColumns.size());
        int lastLevelIndex = levelTables.length - 1;
        QueryTable lastLevel = levelTables[lastLevelIndex];
        while (!columnsToReaggregateBy.isEmpty()) {
            nullColumnNames.addFirst(columnsToReaggregateBy.removeLast().name());
            final TableDefinition lastLevelDefinition = lastLevel.getDefinition();
            final Map<String, Class<?>> nullColumns = nullColumnNames.stream().collect(Collectors.toMap(
                    Function.identity(), ncn -> lastLevelDefinition.getColumn(ncn).getDataType(),
                    Assert::neverInvoked, LinkedHashMap::new));
            lastLevel = lastLevel.aggNoMemo(
                    AggregationProcessor.forRollupReaggregated(aggregations, nullColumns, ROLLUP_COLUMN),
                    false, null, new ArrayList<>(columnsToReaggregateBy));
            --lastLevelIndex;
            levelTables[lastLevelIndex] = lastLevel;
            levelRowLookups[lastLevelIndex] = getRowLookup(lastLevel);
        }
        Assert.eqZero(lastLevelIndex, "lastLevelIndex");
    }

    private static Stream<ColumnDefinition<?>> filterRollupInternalColumns(
            @NotNull final Stream<ColumnDefinition<?>> columnDefinitions) {
        return columnDefinitions.filter(cd -> !cd.getName().endsWith(ROLLUP_COLUMN_SUFFIX));
    }

    @Override
    ChunkSource.WithPrev<? extends Values> makeNodeKeySource(@NotNull final Table nodeKeyTable) {
        return new RollupRowLookupKeySource(
                nodeKeyTable.getColumnSource(getRowDepthColumn().name(), Integer.class),
                groupByColumns.stream()
                        .map(cn -> nodeKeyTable.getColumnSource(cn.name(), root.getColumnSource(cn.name()).getType()))
                        .toArray(ColumnSource[]::new));
    }

    private static int nodeKeyWidth(@Nullable final Object nodeKey) {
        if (nodeKey instanceof Object[]) {
            return ((Object[]) nodeKey).length;
        }
        return 1;
    }

    private static int nodeKeyWidth(final long nodeId) {
        return (int) (nodeId >>> 32);
    }

    private static int nodeSlot(final long nodeId) {
        return (int) nodeId;
    }

    private static long makeNodeId(final int nodeKeyWidth, final int nodeSlot) {
        // NB: nodeKeyWidth is an integer in [0, groupByColumns.size()], and nodeSlot is an integer in [0, 1 << 30)
        return ((long) nodeKeyWidth << 32) | nodeSlot;
    }

    @Override
    boolean isRootNodeKey(@Nullable final Object nodeKey) {
        return nodeKeyWidth(nodeKey) == 0;
    }

    @Override
    long nodeKeyToNodeId(@Nullable final Object nodeKey) {
        final int nodeKeyWidth = nodeKeyWidth(nodeKey);
        if (nodeKeyWidth >= numLevels) {
            return nullNodeId();
        }
        if (nodeKeyWidth == baseLevelIndex && !includesConstituents) {
            return nullNodeId();
        }

        final AggregationRowLookup rowLookup = levelRowLookups[nodeKeyWidth];
        final int nodeSlot = rowLookup.get(nodeKey);
        if (nodeSlot == rowLookup.noEntryValue()) {
            return nullNodeId();
        }

        return makeNodeId(nodeKeyWidth, nodeSlot);
    }

    @Override
    long nullNodeId() {
        return NULL_ROW_KEY;
    }

    @Override
    long rootNodeId() {
        return 0;
    }

    @Override
    long findRowKeyInParentUnsorted(final long childNodeId, @Nullable final Object childNodeKey,
            final boolean usePrev) {
        return childNodeId == nullNodeId() ? NULL_ROW_KEY : nodeSlot(childNodeId);
    }

    @Override
    @Nullable
    Boolean findParentNodeKey(
            @Nullable final Object childNodeKey,
            final long childRowKeyInParentUnsorted,
            final boolean usePrev,
            @NotNull final MutableObject<Object> parentNodeKeyHolder) {
        final int nodeKeyWidth = nodeKeyWidth(childNodeKey);
        switch (nodeKeyWidth) {
            case 0:
                return null;
            case 1:
                parentNodeKeyHolder.setValue(AggregationRowLookup.EMPTY_KEY);
                return true;
            default:
                if (nodeKeyWidth > groupByColumns.size()) {
                    throw new IllegalArgumentException("Invalid node key " + Arrays.toString((Object[]) childNodeKey)
                            + ": wider than maximum " + groupByColumns.size());
                }
                // noinspection ConstantConditions (null falls under "case 1")
                parentNodeKeyHolder.setValue(Arrays.copyOf(((Object[]) childNodeKey), nodeKeyWidth - 1));
                return true;
        }
    }

    @Override
    @Nullable
    Table nodeIdToNodeBaseTable(final long nodeId) {
        final int nodeKeyWidth = nodeKeyWidth(nodeId);
        if (nodeKeyWidth < groupByColumns.size() || includesConstituents()) {
            final int nodeSlot = nodeSlot(nodeId);
            return levelNodeTableSources[nodeKeyWidth].get(nodeSlot);
        }
        return null;
    }

    private RollupNodeOperationsRecorder nodeOperations(final long nodeId) {
        final int nodeKeyWidth = nodeKeyWidth(nodeId);
        if (nodeKeyWidth == groupByColumns.size()) {
            // We must be including constituents
            return constituentNodeOperations;
        }
        return aggregatedNodeOperations;
    }

    @Override
    boolean hasNodeFiltersToApply(long nodeId) {
        return false;
    }

    @Override
    Table applyNodeFormatsAndFilters(final long nodeId, @NotNull final Table nodeBaseTable) {
        return BaseNodeOperationsRecorder.applyFormats(nodeOperations(nodeId), nodeBaseTable);
        // NB: There is no node-level filtering for rollups
    }

    @Override
    Table applyNodeSorts(final long nodeId, @NotNull final Table nodeFilteredTable) {
        return BaseNodeOperationsRecorder.applySorts(nodeOperations(nodeId), nodeFilteredTable);
    }

    @Override
    @NotNull
    ChunkSource.WithPrev<? extends Values>[] makeOrFillChunkSourceArray(
            @NotNull final SnapshotState snapshotState,
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

        final int levelIndex = nodeKeyWidth(nodeId);
        Assert.eq(levelIndex, "levelIndex", snapshotState.getCurrentDepth(), "current snapshot traversal depth");
        final boolean isBaseLevel = levelIndex == baseLevelIndex;
        Assert.eq(isBaseLevel, "isBaseLevel", includesConstituents, "includesConstituents");

        final BitSet columns = snapshotState.getColumns();
        for (int ci = columns.nextSetBit(0); ci >= 0; ci = columns.nextSetBit(ci)) {
            if (result[ci] != null || ci == ROW_EXPANDED_COLUMN_INDEX) {
                continue;
            }
            if (ci == ROW_DEPTH_COLUMN_INDEX) {
                result[ci] = getDepthSource(levelIndex + 1);
            } else if (ci < firstConstituentColumnIndex) {
                final ColumnDefinition<?> cd =
                        aggregatedNodeDefinition.getColumns().get(ci - FIRST_AGGREGATED_COLUMN_INDEX);
                result[ci] = isBaseLevel
                        ? NullValueColumnSource.getInstance(cd.getDataType(), cd.getComponentType())
                        : nodeSortedTable.getColumnSource(cd.getName(), cd.getDataType());
            } else {
                final ColumnDefinition<?> cd =
                        constituentNodeDefinition.getColumns().get(ci - firstConstituentColumnIndex);
                result[ci] = isBaseLevel
                        ? nodeSortedTable.getColumnSource(cd.getName(), cd.getDataType())
                        : NullValueColumnSource.getInstance(cd.getDataType(), cd.getComponentType());
            }
        }
        return result;
    }

    @Override
    NotificationStepSource[] getSourceDependencies() {
        return new NotificationStepSource[] {source};
    }

    @Override
    void maybeWaitForStructuralSatisfaction() {
        // NB: It's sufficient to wait for the root node, which is done at the beginning of traversal.
    }
}
