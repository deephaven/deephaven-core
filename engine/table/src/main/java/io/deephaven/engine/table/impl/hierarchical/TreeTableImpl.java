package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Partition;
import io.deephaven.base.StringUtils;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static io.deephaven.engine.table.Table.*;
import static io.deephaven.engine.table.impl.partitioned.PartitionedTableCreatorImpl.CONSTITUENT;

/**
 * {@link RollupTable} implementation.
 */
public class TreeTableImpl extends HierarchicalTableImpl<TreeTable, TreeTableImpl> implements TreeTable {

    public static final ColumnName TREE_COLUMN = ColumnName.of("__TREE_HIERARCHY__");
    public static final ColumnName REVERSE_LOOKUP_ROW_KEY_COLUMN = ColumnName.of("__ROW_KEY__");

    private final ColumnName identifierColumn;
    private final ColumnName parentIdentifierColumn;
    private final TreeNodeOperationsRecorder nodeOperations;

    public TreeTableImpl(
            @NotNull final QueryTable source,
            @NotNull final QueryTable root,
            @NotNull final ColumnName identifierColumn,
            @NotNull final ColumnName parentIdentifierColumn,
            @Nullable final TreeNodeOperationsRecorder nodeOperations) {
        super(source, root);
        this.identifierColumn = identifierColumn;
        this.parentIdentifierColumn = parentIdentifierColumn;
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
    public NodeOperationsRecorder makeNodeOperationsRecorder() {
        return new TreeNodeOperationsRecorder(root.getDefinition());
    }

    @Override
    public TreeTable withNodeOperations(@NotNull final NodeOperationsRecorder nodeOperations) {
        return new TreeTableImpl(source, root, identifierColumn, parentIdentifierColumn,
                accumulateOperations(this.nodeOperations, (TreeNodeOperationsRecorder) nodeOperations));
    }

    @Override
    public TreeTable withNodeFilterColumns(@NotNull Collection<? extends ColumnName> columns) {
        return null;
    }

    private static TreeNodeOperationsRecorder accumulateOperations(
            @Nullable final TreeNodeOperationsRecorder existing,
            @NotNull final TreeNodeOperationsRecorder added) {
        return existing == null ? added : existing.withOperations(added);
    }

    @Override
    public TreeTable reapply(@NotNull final UnaryOperator<Table> sourceTransformer) {
        // TODO-RWC: Make sure we handle special filtering logic
        final Table newSource = sourceTransformer.apply(source);
        if (!source.getDefinition().equals(newSource.getDefinition())) {
            throw new UnsupportedOperationException(
                    "Unexpected definition change: " +
                            source.getDefinition().getDifferenceDescription(
                                    newSource.getDefinition(), "original source", "new source", ", "));
        }
        // TODO-RWC: Attribute copies?
        final TreeTable tree = newSource.tree(identifierColumn.name(), parentIdentifierColumn.name());
        return tree.withNodeOperations(nodeOperations);
    }

    @Override
    protected TreeTableImpl copy() {
        final TreeTableImpl result =
                new TreeTableImpl(source, root, identifierColumn, parentIdentifierColumn, nodeOperations);
        LiveAttributeMap.copyAttributes(this, result, ak -> true);
        return result;
    }

    public static TreeTable makeTree(@NotNull final QueryTable input,
                           @NotNull final ColumnName identifierColumn,
                           @NotNull final ColumnName parentIdentifierColumn) {
        final Table partitioned;
        {
            final ColumnDefinition parentIdColumnDefinition = definition.getColumn(parentColumn);
            final TableDefinition parentIdOnlyTableDefinition = TableDefinition.of(parentIdColumnDefinition);
            final Table nullParent = new QueryTable(parentIdOnlyTableDefinition, RowSetFactory.flat(1).toTracking(),
                    NullValueColumnSource.createColumnSourceMap(parentIdOnlyTableDefinition), null, null);
            partitioned = aggNoMemo(AggregationProcessor.forAggregation(List.of(Partition.of(CONSTITUENT))),
                    true, nullParent, ColumnName.from(parentColumn));
        }
        // This is "safe" because we rely on the implementation details of aggregation and the partition operator.
        final QueryTable rootTable = (QueryTable) partitioned.getColumnSource(CONSTITUENT.name()).get(0);

        final Table result = BaseHierarchicalTable.createFrom((QueryTable) rootTable.copy(),
                new TreeTableInfo(idColumn, parentColumn));

        // If the parent table has an RLL attached to it, we can re-use it.
        final ReverseLookup reverseLookup;
        if (hasAttribute(PREPARED_RLL_ATTRIBUTE)) {
            reverseLookup = (ReverseLookup) Objects.requireNonNull(getAttribute(PREPARED_RLL_ATTRIBUTE));
            final String[] listenerCols = reverseLookup.getKeyColumns();
            if (listenerCols.length != 1 || !listenerCols[0].equals(idColumn)) {
                final String listenerColError =
                        StringUtils.joinStrings(Arrays.stream(listenerCols).map(col -> "'" + col + "'"), ", ");
                throw new IllegalStateException(
                        "Table was prepared for tree with a different identifier column. Expected `" + idColumn
                                + "`, Actual " + listenerColError);
            }
        } else {
            reverseLookup = ReverseLookupListener.makeReverseLookupListenerWithSnapshot(QueryTable.this, idColumn);
        }

        result.setAttribute(HIERARCHICAL_CHILDREN_TABLE_ATTRIBUTE, partitioned);
        result.setAttribute(HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE, QueryTable.this);
        result.setAttribute(REVERSE_LOOKUP_ATTRIBUTE, reverseLookup);
        copyAttributes(result, BaseTable.CopyAttributeOperation.Tree);
        maybeUpdateSortableColumns(result);

        return result;
    }
}
