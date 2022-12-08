package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Partition;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.deephaven.engine.table.impl.BaseTable.shouldCopyAttribute;
import static io.deephaven.engine.table.impl.by.AggregationProcessor.getRowLookup;
import static io.deephaven.engine.table.impl.partitioned.PartitionedTableCreatorImpl.CONSTITUENT;

/**
 * {@link RollupTable} implementation.
 */
public class TreeTableImpl extends HierarchicalTableImpl<TreeTable, TreeTableImpl> implements TreeTable {

    private static final ColumnName TREE_COLUMN = ColumnName.of("__TREE__");
    private static final ColumnName EXPANDABLE_COLUMN = ColumnName.of("__EXPANDABLE__");
    private static final int EXPANDABLE_COLUMN_INDEX = 0;
    private static final ColumnName DEPTH_COLUMN = ColumnName.of("__DEPTH__");
    private static final int DEPTH_COLUMN_INDEX = 1;
    public static final ColumnName REVERSE_LOOKUP_ROW_KEY_COLUMN = ColumnName.of("__ROW_KEY__");

    private final ColumnSource<?> sourceParentIdSource;
    private final QueryTable tree;
    private final AggregationRowLookup treeRowLookup;

    private final ColumnSource<Table> treeNodeTableSource;
    private final TreeReverseLookup reverseLookup;
    private final boolean filtered;
    private final ColumnName identifierColumn;
    private final ColumnName parentIdentifierColumn;
    private final Set<ColumnName> nodeFilterColumns;
    private final TreeNodeOperationsRecorder nodeOperations;

    private TreeTableImpl(
            @NotNull final Map<String, Object> initialAttributes,
            @NotNull final QueryTable source,
            @NotNull final QueryTable tree,
            @NotNull final TreeReverseLookup reverseLookup,
            @NotNull final ColumnName identifierColumn,
            @NotNull final ColumnName parentIdentifierColumn,
            @NotNull final Set<ColumnName> nodeFilterColumns,
            @Nullable final TreeNodeOperationsRecorder nodeOperations) {
        super(initialAttributes, source, getTreeRoot(tree));
        if (source.isRefreshing()) {
            manage(tree);
            manage(reverseLookup);
        }
        sourceParentIdSource = source.getColumnSource(parentIdentifierColumn.name());
        this.tree = tree;
        treeRowLookup = AggregationProcessor.getRowLookup(tree);
        treeNodeTableSource = tree.getColumnSource(TREE_COLUMN.name(), Table.class);
        this.reverseLookup = reverseLookup;
        filtered = !reverseLookup.sameSource(source);
        this.identifierColumn = identifierColumn;
        this.parentIdentifierColumn = parentIdentifierColumn;
        this.nodeFilterColumns = nodeFilterColumns;
        this.nodeOperations = nodeOperations;

        if (source.isRefreshing()) {
            Assert.assertion(tree.isRefreshing(), "tree.isRefreshing() if source.isRefreshing()");
            // The tree aggregation result depends on the source and all the node tables.
            manage(tree);
            // The reverse lookup just depends on the (original, unfiltered) source, which may not be our direct source.
            manage(reverseLookup);
        }
    }

    @Override
    public ColumnName getIdentifierColumn() {
        return identifierColumn;
    }

    @Override
    public ColumnName getParentIdentifierColumn() {
        return parentIdentifierColumn;
    }

    @Override
    public ColumnName getExpandableColumn() {
        return EXPANDABLE_COLUMN;
    }

    @Override
    public ColumnName getDepthColumn() {
        return DEPTH_COLUMN;
    }

    @Override
    public TableDefinition getNodeDefinition() {
        return nodeOperations == null ? source.getDefinition() : nodeOperations.getResultDefinition();
    }

    @Override
    public TreeTable withNodeFilterColumns(@NotNull final Collection<? extends ColumnName> columns) {
        final Set<ColumnName> resultNodeFilterColumns = new HashSet<>(nodeFilterColumns);
        resultNodeFilterColumns.addAll(columns);
        return new TreeTableImpl(getAttributes(), source, tree, reverseLookup, identifierColumn, parentIdentifierColumn,
                Collections.unmodifiableSet(resultNodeFilterColumns), nodeOperations);
    }

    @Override
    public TreeTable withFilters(@NotNull Collection<? extends Filter> filters) {
        if (filters.isEmpty()) {
            return noopResult();
        }

        final WhereFilter[] whereFilters = WhereFilter.from(filters);
        final Map<Boolean, List<WhereFilter>> nodeSuitabilityToFilters = Stream.of(whereFilters)
                .peek(wf -> wf.init(source.getDefinition()))
                .collect(Collectors.partitioningBy(wf -> {
                    // Node-level filters have only node-filter columns and use no column arrays
                    return wf.getColumns().stream().map(ColumnName::of).allMatch(nodeFilterColumns::contains)
                            && wf.getColumnArrays().isEmpty();
                }));
        final List<WhereFilter> nodeFilters = nodeSuitabilityToFilters.get(true);
        final List<WhereFilter> sourceFilters = nodeSuitabilityToFilters.get(false);

        final NodeOperationsRecorder nodeFiltersRecorder =
                nodeFilters.isEmpty() ? null : makeNodeOperationsRecorder().where(nodeFilters);
        if (sourceFilters.isEmpty()) {
            Assert.neqNull(nodeFiltersRecorder, "nodeFiltersRecorder");
            return withNodeOperations(makeNodeOperationsRecorder().where(nodeFilters));
        }

        final QueryTable filteredSource = (QueryTable) source.apply(
                new TreeTableFilter.Operator(this, sourceFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY)));
        final QueryTable filteredTree = computeTree(filteredSource, parentIdentifierColumn);
        return new TreeTableImpl(getAttributes(), filteredSource, filteredTree, reverseLookup, identifierColumn,
                parentIdentifierColumn, nodeFilterColumns, accumulateOperations(nodeOperations, nodeFiltersRecorder));
    }

    /**
     * @return The TreeReverseLookup for this TreeTableImpl
     */
    TreeReverseLookup getReverseLookup() {
        return reverseLookup;
    }

    @Override
    public NodeOperationsRecorder makeNodeOperationsRecorder() {
        return new TreeNodeOperationsRecorder(getNodeDefinition());
    }

    @Override
    public TreeTable withNodeOperations(@NotNull final NodeOperationsRecorder nodeOperations) {
        return new TreeTableImpl(getAttributes(), source, tree, reverseLookup, identifierColumn, parentIdentifierColumn,
                nodeFilterColumns, accumulateOperations(this.nodeOperations, nodeOperations));
    }

    private static TreeNodeOperationsRecorder accumulateOperations(
            @Nullable final TreeNodeOperationsRecorder existing,
            @Nullable final NodeOperationsRecorder added) {
        if (added == null) {
            return existing;
        }
        final TreeNodeOperationsRecorder addedTyped = (TreeNodeOperationsRecorder) added;
        return existing == null ? addedTyped : existing.withOperations(addedTyped);
    }

    @Override
    protected TreeTableImpl copy() {
        return new TreeTableImpl(getAttributes(), source, tree, reverseLookup, identifierColumn, parentIdentifierColumn,
                nodeFilterColumns, nodeOperations);
    }

    public static TreeTable makeTree(
            @NotNull final QueryTable source,
            @NotNull final ColumnName identifierColumn,
            @NotNull final ColumnName parentIdentifierColumn) {
        final QueryTable tree = computeTree(source, parentIdentifierColumn);
        final QueryTable reverseLookupTable = computeReverseLookupTable(source, identifierColumn);
        final TreeReverseLookup reverseLookup = new TreeReverseLookup(source, reverseLookupTable);
        final TreeTableImpl result = new TreeTableImpl(
                source.getAttributes(ak -> shouldCopyAttribute(ak, BaseTable.CopyAttributeOperation.Tree)),
                source, tree, reverseLookup, identifierColumn, parentIdentifierColumn, Set.of(), null);
        source.copySortableColumns(result, (final String columnName) -> true);
        return result;
    }

    private static QueryTable computeTree(
            @NotNull final QueryTable source,
            @NotNull final ColumnName parentIdColumn) {
        final ColumnDefinition parentIdColumnDefinition = source.getDefinition().getColumn(parentIdColumn.name());
        final TableDefinition parentIdOnlyTableDefinition = TableDefinition.of(parentIdColumnDefinition);
        final Table nullParent = new QueryTable(parentIdOnlyTableDefinition, RowSetFactory.flat(1).toTracking(),
                NullValueColumnSource.createColumnSourceMap(parentIdOnlyTableDefinition), null, null);
        return source.aggNoMemo(AggregationProcessor.forAggregation(List.of(Partition.of(TREE_COLUMN))),
                true, nullParent, List.of(parentIdColumn));
    }

    private static QueryTable getTreeRoot(@NotNull final QueryTable tree) {
        // NB: This is "safe" because we rely on the implementation details of aggregation and the partition operator,
        // which ensure that the initial groups are bucketed first and the result row set is flat.
        return (QueryTable) tree.getColumnSource(CONSTITUENT.name()).get(0);
    }

    private static QueryTable computeReverseLookupTable(
            @NotNull final QueryTable source,
            @NotNull final ColumnName idColumn) {
        return source.aggNoMemo(AggregationProcessor.forTreeReverseLookup(), false, null, List.of(idColumn));
    }

    @Override
    ChunkSource.WithPrev<? extends Values> makeNodeKeySource(@NotNull final Table nodeKeyTable) {
        return ReinterpretUtils.maybeConvertToPrimitive(nodeKeyTable.getColumnSource(identifierColumn.name()));
    }

    @Override
    @Nullable
    Boolean nodeKeyToParentNodeKey(
            @Nullable final Object childNodeKey,
            final boolean usePrev,
            @NotNull final MutableObject<Object> parentNodeKeyHolder) {
        if (isRootNodeKey(childNodeKey)) {
            return null;
        }
        final long sourceRowKey = usePrev
                ? reverseLookup.getPrev(childNodeKey)
                : reverseLookup.get(childNodeKey);
        if (sourceRowKey == reverseLookup.noEntryValue()) {
            return false;
        }
        if (filtered) {
            final long sourceRowPosition = usePrev
                    ? getSource().getRowSet().findPrev(sourceRowKey)
                    : getSource().getRowSet().find(sourceRowKey);
            if (sourceRowPosition == RowSequence.NULL_ROW_KEY) {
                return false;
            }
        }
        final Object parentNodeKey = usePrev
                ? sourceParentIdSource.getPrev(sourceRowKey)
                : sourceParentIdSource.get(sourceRowKey);
        parentNodeKeyHolder.setValue(parentNodeKey);
        return true;
    }

    @Override
    boolean isRootNodeKey(@Nullable final Object nodeKey) {
        return nodeKey == null;
    }

    @Override
    Object getRootNodeKey() {
        return null;
    }

    @Override
    long nodeKeyToNodeId(@Nullable final Object nodeKey) {
        return treeRowLookup.get(nodeKey);
    }

    ColumnSource<Table> getTreeNodeTableSource() {
        return treeNodeTableSource;
    }

    @Override
    @Nullable
    Table nodeIdToNodeBaseTable(final long nodeId) {
        return treeNodeTableSource.get(nodeId);
    }

    @Override
    Table applyNodeFormatsAndFilters(final long nodeId, @NotNull final Table nodeBaseTable) {
        final Table nodeFormattedTable = BaseNodeOperationsRecorder.applyFormats(nodeOperations, nodeBaseTable);
        return TreeNodeOperationsRecorder.applyFilters(nodeOperations, nodeFormattedTable);
    }

    @Override
    Table applyNodeSorts(final long nodeId, @NotNull final Table nodeFilteredTable) {
        return BaseNodeOperationsRecorder.applySorts(nodeOperations, nodeFilteredTable);
    }

    @NotNull
    @Override
    ChunkSource.WithPrev<? extends Values>[] makeOrFillChunkSourceArray(
            @NotNull final SnapshotStateImpl snapshotState,
            final long nodeId,
            @NotNull final Table nodeSortedTable,
            @Nullable final ChunkSource.WithPrev<? extends Values>[] existingChunkSources) {
        // We have 2 extra columns per row:
        // 1. "expandable" -> boolean, is it possible to expand this row?
        // 2. "depth" -> int, how deep is this row in the tree?
        // These are at index 0 and 1, respectively, followed by the node columns.
        final int numColumns = getNodeDefinition().numColumns() + 2;
        final ChunkSource.WithPrev<? extends Values>[] result;
        if (existingChunkSources != null) {
            Assert.eq(existingChunkSources.length, "existingChunkSources.length", numColumns, "numColumns");
            result = existingChunkSources;
        } else {
            //noinspection unchecked
            result = new ChunkSource.WithPrev[numColumns];
        }
        // TODO-RWC: Continue here
        (snapshotState.getColumns(). == null ? IntStream.range(0, numColumns) : snapshotState.getColumns().stream())
                .forEach(ci -> {
            switch (ci) {
                case EXPANDABLE_COLUMN_INDEX:
                case DEPTH_COLUMN_INDEX:
                default:
            }
        });
        return result;
    }

    @Override
    NotificationStepSource[] getSourceDependencies() {
        // NB: The reverse lookup may be derived from an unfiltered parent of our source, hence we need to treat it as a
        // separate dependency if we're filtered.
        return filtered
                ? new NotificationStepSource[] {source, reverseLookup}
                : new NotificationStepSource[] {source};
    }

    @Override
    void maybeWaitForStructuralSatisfaction() {
        // NB: Our root is just a node in the tree (which is a partitioned table of constituent nodes), so waiting for
        // satisfaction of the root would be insufficient (and unnecessary if we're waiting for the tree).
        maybeWaitForSatisfaction(tree);
    }

    static final class TreeReverseLookup
            extends LivenessArtifact
            implements ReverseLookup, NotificationStepSource {

        private final Object source;
        private final NotificationStepSource parent;
        private final AggregationRowLookup rowLookup;
        private final ColumnSource<Long> sourceRowKeyColumnSource;

        private TreeReverseLookup(@NotNull final Object source, @NotNull final QueryTable reverseLookupTable) {
            this.source = source;
            if (reverseLookupTable.isRefreshing()) {
                parent = reverseLookupTable;
                manage(reverseLookupTable);
            } else {
                parent = null;
            }
            rowLookup = getRowLookup(reverseLookupTable);
            sourceRowKeyColumnSource =
                    reverseLookupTable.getColumnSource(REVERSE_LOOKUP_ROW_KEY_COLUMN.name(), long.class);
        }

        private boolean sameSource(@NotNull final Object source) {
            return this.source == source;
        }

        @Override
        public long get(final Object nodeKey) {
            final int idAggregationRow = rowLookup.get(nodeKey);
            if (idAggregationRow == rowLookup.noEntryValue()) {
                return this.noEntryValue();
            }
            return sourceRowKeyColumnSource.get(idAggregationRow);
        }

        @Override
        public long getPrev(final Object nodeKey) {
            final int idAggregationRow = rowLookup.get(nodeKey);
            if (idAggregationRow == rowLookup.noEntryValue()) {
                return this.noEntryValue();
            }
            return sourceRowKeyColumnSource.getPrev(idAggregationRow);
        }

        @Override
        public long getLastNotificationStep() {
            return parent.getLastNotificationStep();
        }

        @Override
        public boolean satisfied(final long step) {
            return parent.satisfied(step);
        }
    }
}
