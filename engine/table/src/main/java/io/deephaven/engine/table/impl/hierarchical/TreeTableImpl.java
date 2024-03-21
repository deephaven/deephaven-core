//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Partition;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.BaseTable.shouldCopyAttribute;
import static io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl.LevelExpandable.Undetermined;
import static io.deephaven.engine.table.impl.sources.ReinterpretUtils.maybeConvertToPrimitive;

/**
 * {@link RollupTable} implementation.
 */
public class TreeTableImpl extends HierarchicalTableImpl<TreeTable, TreeTableImpl> implements TreeTable {

    private static final ColumnName TREE_COLUMN = ColumnName.of("__TREE__");

    private final ColumnSource<?> sourceParentIdSource;
    private final QueryTable tree;
    private final AggregationRowLookup treeRowLookup;

    private final ColumnSource<Table> treeNodeTableSource;
    private final TreeSourceRowLookup sourceRowLookup;
    private final boolean filtered;
    private final ColumnName identifierColumn;
    private final ColumnName parentIdentifierColumn;
    private final Set<ColumnName> nodeFilterColumns;
    private final TreeNodeOperationsRecorder nodeOperations;
    private final List<ColumnDefinition<?>> availableColumnDefinitions;

    private TreeTableImpl(
            @NotNull final Map<String, Object> initialAttributes,
            @NotNull final QueryTable source,
            @NotNull final QueryTable tree,
            @NotNull final TreeSourceRowLookup sourceRowLookup,
            @NotNull final ColumnName identifierColumn,
            @NotNull final ColumnName parentIdentifierColumn,
            @NotNull final Set<ColumnName> nodeFilterColumns,
            @Nullable final TreeNodeOperationsRecorder nodeOperations,
            @Nullable final List<ColumnDefinition<?>> availableColumnDefinitions) {
        super(initialAttributes, source, getTreeRoot(tree));
        if (source.isRefreshing()) {
            manage(tree);
            manage(sourceRowLookup);
        }
        sourceParentIdSource = source.getColumnSource(parentIdentifierColumn.name());
        this.tree = tree;
        treeRowLookup = AggregationProcessor.getRowLookup(tree);
        treeNodeTableSource = tree.getColumnSource(TREE_COLUMN.name(), Table.class);
        this.sourceRowLookup = sourceRowLookup;
        filtered = !sourceRowLookup.sameSource(source);
        this.identifierColumn = identifierColumn;
        this.parentIdentifierColumn = parentIdentifierColumn;
        this.nodeFilterColumns = nodeFilterColumns;
        this.nodeOperations = nodeOperations;
        this.availableColumnDefinitions = availableColumnDefinitions == null
                ? computeAvailableColumnDefinitions(getNodeDefinition())
                : availableColumnDefinitions;

        if (source.isRefreshing()) {
            Assert.assertion(tree.isRefreshing(), "tree.isRefreshing() if source.isRefreshing()");
            // The tree aggregation result depends on the source and all the node tables.
            manage(tree);
            // The reverse lookup just depends on the (original, unfiltered) source, which may not be our direct source.
            manage(sourceRowLookup);
        }
    }

    @Override
    public String getDescription() {
        return "TreeTable(" + source.getDescription()
                + ", " + identifierColumn.name()
                + ", " + parentIdentifierColumn.name()
                + ")";
    }

    @Override
    public Table getEmptyExpansionsTable() {
        return makeNullSingleColumnTable(getSource(), getIdentifierColumn(), 0);
    }

    private static Table makeNullSingleColumnTable(
            @NotNull final Table source,
            @NotNull final ColumnName column,
            final long size) {
        final ColumnDefinition<?> columnDefinition = source.getDefinition().getColumn(column.name());
        final TableDefinition columnOnlyTableDefinition = TableDefinition.of(columnDefinition);
        // noinspection resource
        return new QueryTable(columnOnlyTableDefinition, RowSetFactory.flat(size).toTracking(),
                NullValueColumnSource.createColumnSourceMap(columnOnlyTableDefinition), null, null);
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
    public TableDefinition getNodeDefinition() {
        return nodeOperations == null ? source.getDefinition() : nodeOperations.getResultDefinition();
    }

    @Override
    public List<ColumnDefinition<?>> getAvailableColumnDefinitions() {
        return availableColumnDefinitions;
    }

    private static List<ColumnDefinition<?>> computeAvailableColumnDefinitions(
            @NotNull final TableDefinition nodeDefinition) {
        return Stream.concat(STRUCTURAL_COLUMN_DEFINITIONS.stream(), nodeDefinition.getColumnStream())
                .collect(Collectors.toList());
    }

    @Override
    public TreeTable withNodeFilterColumns(@NotNull final Collection<? extends ColumnName> columns) {
        final Set<ColumnName> resultNodeFilterColumns = new HashSet<>(nodeFilterColumns);
        resultNodeFilterColumns.addAll(columns);
        return new TreeTableImpl(getAttributes(), source, tree, sourceRowLookup, identifierColumn,
                parentIdentifierColumn,
                Collections.unmodifiableSet(resultNodeFilterColumns), nodeOperations, availableColumnDefinitions);
    }

    @Override
    public TreeTable withFilter(@NotNull Filter filter) {
        final WhereFilter[] whereFilters = WhereFilter.fromInternal(filter);
        if (whereFilters.length == 0) {
            return noopResult();
        }
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        final Map<Boolean, List<WhereFilter>> nodeSuitabilityToFilters = Stream.of(whereFilters)
                .peek(wf -> wf.init(source.getDefinition(), compilationProcessor))
                .collect(Collectors.partitioningBy(wf -> {
                    // Node-level filters have only node-filter columns and use no column arrays
                    return wf.getColumns().stream().map(ColumnName::of).allMatch(nodeFilterColumns::contains)
                            && wf.getColumnArrays().isEmpty();
                }));
        compilationProcessor.compile();

        final List<WhereFilter> nodeFilters = nodeSuitabilityToFilters.get(true);
        final List<WhereFilter> sourceFilters = nodeSuitabilityToFilters.get(false);

        final NodeOperationsRecorder nodeFiltersRecorder =
                nodeFilters.isEmpty() ? null : makeNodeOperationsRecorder().where(Filter.and(nodeFilters));
        if (sourceFilters.isEmpty()) {
            Assert.neqNull(nodeFiltersRecorder, "nodeFiltersRecorder");
            return withNodeOperations(makeNodeOperationsRecorder().where(Filter.and(nodeFilters)));
        }

        final QueryTable filteredSource = (QueryTable) source.apply(
                new TreeTableFilter.Operator(this, sourceFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY)));
        final QueryTable filteredTree = computeTree(filteredSource, parentIdentifierColumn);
        return new TreeTableImpl(getAttributes(), filteredSource, filteredTree, sourceRowLookup, identifierColumn,
                parentIdentifierColumn, nodeFilterColumns, accumulateOperations(nodeOperations, nodeFiltersRecorder),
                availableColumnDefinitions);
    }

    /**
     * @return The TreeSourceRowLookup for this TreeTableImpl
     */
    TreeSourceRowLookup getSourceRowLookup() {
        return sourceRowLookup;
    }

    @Override
    public NodeOperationsRecorder makeNodeOperationsRecorder() {
        return new TreeNodeOperationsRecorder(getNodeDefinition());
    }

    @Override
    public TreeTable withNodeOperations(@NotNull final NodeOperationsRecorder nodeOperations) {
        if (nodeOperations.isEmpty()) {
            return noopResult();
        }
        return new TreeTableImpl(getAttributes(), source, tree, sourceRowLookup, identifierColumn,
                parentIdentifierColumn, nodeFilterColumns, accumulateOperations(this.nodeOperations, nodeOperations),
                ((TreeNodeOperationsRecorder) nodeOperations).getRecordedFormats().isEmpty()
                        ? availableColumnDefinitions
                        : null);
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
        return new TreeTableImpl(getAttributes(), source, tree, sourceRowLookup, identifierColumn,
                parentIdentifierColumn, nodeFilterColumns, nodeOperations, availableColumnDefinitions);
    }

    public static TreeTable makeTree(
            @NotNull final QueryTable source,
            @NotNull final ColumnName identifierColumn,
            @NotNull final ColumnName parentIdentifierColumn) {
        final QueryTable tree = computeTree(source, parentIdentifierColumn);
        final QueryTable sourceRowLookupTable = computeSourceRowLookupTable(source, identifierColumn);
        final TreeSourceRowLookup sourceRowLookup = new TreeSourceRowLookup(source, sourceRowLookupTable);
        final TreeTableImpl result = new TreeTableImpl(
                source.getAttributes(ak -> shouldCopyAttribute(ak, BaseTable.CopyAttributeOperation.Tree)),
                source, tree, sourceRowLookup, identifierColumn, parentIdentifierColumn, Set.of(), null, null);
        source.copySortableColumns(result, (final String columnName) -> true);
        return result;
    }

    private static QueryTable computeTree(
            @NotNull final QueryTable source,
            @NotNull final ColumnName parentIdColumn) {
        return source.aggNoMemo(AggregationProcessor.forAggregation(List.of(Partition.of(TREE_COLUMN))),
                true, makeNullSingleColumnTable(source, parentIdColumn, 1), List.of(parentIdColumn));
    }

    private static QueryTable getTreeRoot(@NotNull final QueryTable tree) {
        // NB: This is "safe" because we rely on the implementation details of aggregation and the partition operator,
        // which ensure that the initial groups are bucketed first and the result row set is flat.
        return (QueryTable) tree.getColumnSource(TREE_COLUMN.name()).get(0);
    }

    private static QueryTable computeSourceRowLookupTable(
            @NotNull final QueryTable source,
            @NotNull final ColumnName idColumn) {
        return source.aggNoMemo(AggregationProcessor.forTreeSourceRowLookup(), false, null, List.of(idColumn));
    }

    @Override
    Iterable<Object> getDefaultExpansionNodeKeys() {
        return Collections.singletonList(null);
    }

    @Override
    ChunkSource.WithPrev<? extends Values> makeNodeKeySource(@NotNull final Table nodeKeyTable) {
        return maybeConvertToPrimitive(
                nodeKeyTable.getColumnSource(identifierColumn.name(),
                        getRoot().getColumnSource(identifierColumn.name()).getType()));
    }

    @Override
    boolean isRootNodeKey(@Nullable final Object nodeKey) {
        return nodeKey == null;
    }

    @Override
    long nodeKeyToNodeId(@Nullable final Object nodeKey) {
        return treeRowLookup.get(nodeKey);
    }

    @Override
    long nullNodeId() {
        return treeRowLookup.noEntryValue();
    }

    @Override
    long rootNodeId() {
        return 0;
    }

    @Override
    long findRowKeyInParentUnsorted(
            final long childNodeId,
            @Nullable final Object childNodeKey,
            final boolean usePrev) {
        final long sourceRowKey = usePrev
                ? sourceRowLookup.getPrev(childNodeKey)
                : sourceRowLookup.get(childNodeKey);
        if (sourceRowKey == sourceRowLookup.noEntryValue()) {
            return NULL_ROW_KEY;
        }
        if (filtered) {
            final long sourceRowPosition = usePrev
                    ? getSource().getRowSet().findPrev(sourceRowKey)
                    : getSource().getRowSet().find(sourceRowKey);
            if (sourceRowPosition == NULL_ROW_KEY) {
                return NULL_ROW_KEY;
            }
        }
        return sourceRowKey;
    }

    @Override
    @Nullable
    Boolean findParentNodeKey(
            @Nullable final Object childNodeKey,
            final long childRowKeyInParentUnsorted,
            final boolean usePrev,
            @NotNull final MutableObject<Object> parentNodeKeyHolder) {
        if (isRootNodeKey(childNodeKey)) {
            return null;
        }
        // childRowKeyInParentUnsorted is also the row key in our source table
        if (childRowKeyInParentUnsorted == NULL_ROW_KEY) {
            return false;
        }
        final Object parentNodeKey = usePrev
                ? sourceParentIdSource.getPrev(childRowKeyInParentUnsorted)
                : sourceParentIdSource.get(childRowKeyInParentUnsorted);
        parentNodeKeyHolder.setValue(parentNodeKey);
        return true;
    }

    @Override
    @Nullable
    Table nodeIdToNodeBaseTable(final long nodeId) {
        return treeNodeTableSource.get(nodeId);
    }

    @Override
    boolean hasNodeFiltersToApply(long nodeId) {
        return nodeOperations != null && !nodeOperations.getRecordedFilters().isEmpty();
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

    @Override
    @NotNull
    ChunkSource.WithPrev<? extends Values>[] makeOrFillChunkSourceArray(
            @NotNull final SnapshotStateImpl snapshotState,
            final long nodeId,
            @NotNull final Table nodeSortedTable,
            @Nullable final ChunkSource.WithPrev<? extends Values>[] existingChunkSources) {
        // We have 2 extra columns per row:
        // 1. "depth" -> int, how deep is this row in the tree?
        // 2. "row expanded" -> Boolean, always handled by the parent class, ignored here
        // These are at index 0 and 1, respectively, followed by the node columns.
        final int numColumns = getNodeDefinition().numColumns() + EXTRA_COLUMN_COUNT;
        final ChunkSource.WithPrev<? extends Values>[] result =
                maybeAllocateResultChunkSourceArray(existingChunkSources, numColumns);

        final BitSet columns = snapshotState.getColumns();
        for (int ci = columns.nextSetBit(0); ci >= 0; ci = columns.nextSetBit(ci + 1)) {
            if (ci == ROW_DEPTH_COLUMN_INDEX) {
                // Tree nodes can change depth, so update regardless of existing result
                result[ci] = getDepthSource(snapshotState.getCurrentDepth());
            } else if (result[ci] == null && ci != ROW_EXPANDED_COLUMN_INDEX) {
                final ColumnDefinition<?> cd = getNodeDefinition().getColumns().get(ci - EXTRA_COLUMN_COUNT);
                result[ci] = maybeConvertToPrimitive(nodeSortedTable.getColumnSource(cd.getName(), cd.getDataType()));
            }
        }
        return result;
    }

    @Override
    LevelExpandable levelExpandable(@NotNull final SnapshotStateImpl snapshotState) {
        // We don't have sufficient information to know if any of this level's children are expandable.
        return Undetermined;
    }

    @Override
    @NotNull
    LongUnaryOperator makeChildNodeIdLookup(
            @NotNull final SnapshotStateImpl snapshotState,
            @NotNull final Table nodeTableToExpand,
            final boolean sorted) {
        final ColumnSource<?> childIdentifierSource = nodeTableToExpand.getColumnSource(identifierColumn.name());
        return snapshotState.usePrev()
                ? (final long rowKey) -> nodeKeyToNodeId(childIdentifierSource.getPrev(rowKey))
                : (final long rowKey) -> nodeKeyToNodeId(childIdentifierSource.get(rowKey));
    }

    @Override
    boolean nodeIdExpandable(@NotNull final SnapshotStateImpl snapshotState, final long nodeId) {
        if (nodeId == nullNodeId()) {
            return false;
        }
        final SnapshotStateImpl.NodeTableState nodeTableState = snapshotState.getNodeTableState(nodeId);
        if (nodeTableState == null) {
            return false;
        }
        nodeTableState.ensurePreparedForTraversal();
        final Table traversalTable = nodeTableState.getTraversalTable();
        return (snapshotState.usePrev() ? traversalTable.getRowSet().sizePrev() : traversalTable.size()) > 0;
    }

    @Override
    NotificationStepSource[] getSourceDependencies() {
        // NB: The reverse lookup may be derived from an unfiltered parent of our source, hence we need to treat it as a
        // separate dependency if we're filtered.
        return filtered
                ? new NotificationStepSource[] {source, sourceRowLookup}
                : new NotificationStepSource[] {source};
    }

    @Override
    void maybeWaitForStructuralSatisfaction() {
        // NB: Our root is just a node in the tree (which is a partitioned table of constituent nodes), so waiting for
        // satisfaction of the root would be insufficient (and unnecessary if we're waiting for the tree).
        maybeWaitForSatisfaction(tree);
    }

}
