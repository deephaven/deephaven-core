package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.LiveAttributeMap;
import io.deephaven.engine.table.impl.QueryTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.function.UnaryOperator;

/**
 * {@link RollupTable} implementation.
 */
public class TreeTableImpl extends HierarchicalTableImpl<TreeTable, TreeTableImpl> implements TreeTable {

    public static final ColumnName TREE_COLUMN_NAME = ColumnName.of("__TREE_HIERARCHY__");

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
        return TREE_COLUMN_NAME;
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
}
