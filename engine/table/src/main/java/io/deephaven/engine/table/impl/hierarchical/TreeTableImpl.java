package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Partition;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.engine.table.impl.BaseTable.shouldCopyAttribute;
import static io.deephaven.engine.table.impl.by.AggregationProcessor.getRowLookup;
import static io.deephaven.engine.table.impl.partitioned.PartitionedTableCreatorImpl.CONSTITUENT;

/**
 * {@link RollupTable} implementation.
 */
public class TreeTableImpl extends HierarchicalTableImpl<TreeTable, TreeTableImpl> implements TreeTable {

    public static final ColumnName TREE_COLUMN = ColumnName.of("__TREE_HIERARCHY__");
    public static final ColumnName REVERSE_LOOKUP_ROW_KEY_COLUMN = ColumnName.of("__ROW_KEY__");

    private final QueryTable reverseLookupTable;
    private final ColumnName identifierColumn;
    private final ColumnName parentIdentifierColumn;
    private final Set<ColumnName> nodeFilterColumns;
    private final TreeNodeOperationsRecorder nodeOperations;

    private final ToLongFunction<Object> reverseLookup;

    private TreeTableImpl(
            @NotNull final Map<String, Object> initialAttributes,
            @NotNull final QueryTable source,
            @NotNull final QueryTable root,
            @Nullable final QueryTable reverseLookupTable, // Used for referential integrity only
            @NotNull final ToLongFunction<Object> reverseLookup,
            @NotNull final ColumnName identifierColumn,
            @NotNull final ColumnName parentIdentifierColumn,
            @NotNull final Set<ColumnName> nodeFilterColumns,
            @Nullable final TreeNodeOperationsRecorder nodeOperations) {
        super(initialAttributes, source, root);
        this.reverseLookupTable = reverseLookupTable;
        if (reverseLookupTable != null && reverseLookupTable.isRefreshing()) {
            manage(reverseLookupTable);
        }
        this.reverseLookup = reverseLookup;
        this.identifierColumn = identifierColumn;
        this.parentIdentifierColumn = parentIdentifierColumn;
        this.nodeFilterColumns = nodeFilterColumns;
        this.nodeOperations = nodeOperations;
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
    public ColumnName getTreeColumn() {
        return TREE_COLUMN;
    }

    @Override
    public TreeTable withNodeFilterColumns(@NotNull final Collection<? extends ColumnName> columns) {
        final Set<ColumnName> resultNodeFilterColumns = new HashSet<>(nodeFilterColumns);
        resultNodeFilterColumns.addAll(columns);
        return new TreeTableImpl(getAttributes(), source, root, reverseLookupTable, reverseLookup,
                identifierColumn, parentIdentifierColumn,
                Collections.unmodifiableSet(resultNodeFilterColumns), nodeOperations);
    }

    @Override
    public TreeTable withFilters(@NotNull Collection<? extends Filter> filters) {
        if (filters.isEmpty()) {
            return copy();
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

        final QueryTable filteredSource = source.apply(new TreeTableFilter(source, sourceFilters));
        final QueryTable filteredRoot = computeTreeRoot(filteredSource, parentIdentifierColumn);
        return new TreeTableImpl(getAttributes(), filteredSource, filteredRoot, reverseLookupTable, reverseLookup,
                identifierColumn, parentIdentifierColumn, nodeFilterColumns,
                accumulateOperations(nodeOperations, nodeFiltersRecorder));
    }

    @Override
    public NodeOperationsRecorder makeNodeOperationsRecorder() {
        return new TreeNodeOperationsRecorder(root.getDefinition());
    }

    @Override
    public TreeTable withNodeOperations(@NotNull final NodeOperationsRecorder nodeOperations) {
        return new TreeTableImpl(getAttributes(), source, root, reverseLookupTable, reverseLookup,
                identifierColumn, parentIdentifierColumn, nodeFilterColumns,
                accumulateOperations(this.nodeOperations, nodeOperations));
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
        return new TreeTableImpl(getAttributes(), source, root, reverseLookupTable, reverseLookup,
                identifierColumn, parentIdentifierColumn, nodeFilterColumns, nodeOperations);
    }

    public static TreeTable makeTree(
            @NotNull final QueryTable source,
            @NotNull final ColumnName identifierColumn,
            @NotNull final ColumnName parentIdentifierColumn) {
        final QueryTable root = computeTreeRoot(source, parentIdentifierColumn);
        final QueryTable reverseLookupTable = computeReverseLookupTable(source, identifierColumn);
        final ToLongFunction<Object> reverseLookupFunction = computeReverseLookupFunction(reverseLookupTable);
        // TODO-RWC: update sortable columns
        return new TreeTableImpl(
                source.getAttributes(ak -> shouldCopyAttribute(ak, BaseTable.CopyAttributeOperation.Tree)),
                source, root, reverseLookupTable, reverseLookupFunction, identifierColumn, parentIdentifierColumn,
                Set.of(), null);
    }

    private static QueryTable computeTreeRoot(
            @NotNull final QueryTable source,
            @NotNull final ColumnName parentIdColumn) {
        final ColumnDefinition parentIdColumnDefinition = source.getDefinition().getColumn(parentIdColumn.name());
        final TableDefinition parentIdOnlyTableDefinition = TableDefinition.of(parentIdColumnDefinition);
        final Table nullParent = new QueryTable(parentIdOnlyTableDefinition, RowSetFactory.flat(1).toTracking(),
                NullValueColumnSource.createColumnSourceMap(parentIdOnlyTableDefinition), null, null);
        final Table partitioned = source.aggNoMemo(
                AggregationProcessor.forAggregation(List.of(Partition.of(CONSTITUENT))),
                true, nullParent, List.of(parentIdColumn));
        // This is "safe" because we rely on the implementation details of aggregation and the partition operator,
        // which ensure that the initial groups are bucketed first and the result row set is flat.
        return (QueryTable) partitioned.getColumnSource(CONSTITUENT.name()).get(0);
    }

    private static QueryTable computeReverseLookupTable(
            @NotNull final QueryTable source,
            @NotNull final ColumnName idColumn) {
        return source.aggNoMemo(AggregationProcessor.forTreeReverseLookup(), false, null, List.of(idColumn));
    }

    private static ToLongFunction<Object> computeReverseLookupFunction(@NotNull final QueryTable reverseLookupTable) {
        final ToIntFunction<Object> rowLookupFunction = getRowLookup(reverseLookupTable);
        final ColumnSource<Long> sourceRowKeyColumnSource =
                reverseLookupTable.getColumnSource(REVERSE_LOOKUP_ROW_KEY_COLUMN.name(), long.class);
        return id -> sourceRowKeyColumnSource.get(rowLookupFunction.applyAsInt(id));
    }
}
