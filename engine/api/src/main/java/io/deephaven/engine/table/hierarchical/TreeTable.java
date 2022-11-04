package io.deephaven.engine.table.hierarchical;

import io.deephaven.api.*;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.DynamicNode;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Interface for the result of {@link Table#tree(String, String)} tree} operations.
 */
public interface TreeTable extends HierarchicalTable<TreeTable> {

    /**
     * Get the identifier column from the {@link Table#tree(String, String) tree} operation.
     *
     * @return The identifier column
     */
    ColumnName getIdentifierColumn();

    /**
     * Get the parent identifier column from the {{@link Table#tree(String, String) tree} operation.
     *
     * @return The parent identifier column
     */
    ColumnName getParentIdentifierColumn();

    /**
     * Get the name for the "tree" column that holds the next level node tables in each level of a tree.
     *
     * @return The tree column name
     */
    ColumnName getTreeColumn();

    /**
     * Recorder for node-level operations to be applied when gathering snapshots.
     */
    interface NodeOperationsRecorder extends
            FormatOperationsRecorder<NodeOperationsRecorder>,
            SortOperationsRecorder<NodeOperationsRecorder>,
            FilterOperationsRecorder<NodeOperationsRecorder> {
    }

    /**
     * Get a {@link NodeOperationsRecorder recorder} for per-node operations to apply during snapshots.
     *
     * @return The new recorder
     */
    NodeOperationsRecorder makeNodeOperationsRecorder();

    /**
     * Get a new TreeTable that will apply the {@link NodeOperationsRecorder recorded} operations to nodes when
     * gathering snapshots.
     *
     * @param nodeOperations The node-level operations to apply. Must have been initially supplied by
     *        {@link #makeNodeOperationsRecorder()} from {@code this} TreeTable.
     * @return The new TreeTable
     */
    TreeTable withNodeOperations(@NotNull NodeOperationsRecorder nodeOperations);

    /**
     * Get a new TreeTable with {@code columns} designated for node-level filtering. This means that UI-driven filters
     * on those columns will be applied to the nodes during snapshots. If no node-filter columns are designated, no
     * filters will be handled at node level. If node-filter columns are designated, filters that include other columns
     * will be handled by filtering the source table in a parent-preserving manner and re-applying the tree operation to
     * the result to produce a new TreeTable. Users of orphan promotion or other strategies to govern the structure of
     * the tree should carefully consider the structure of their data before specifying node-filter columns.
     *
     * @param columns The columns to designate
     * @return The new TreeTable
     */
    TreeTable withNodeFilterColumns(@NotNull Collection<? extends ColumnName> columns);

    /**
     * Apply a transformation to the source table, e.g. for filtering, and re-apply the tree operation to produce a new
     * TreeTable inheriting the same node operations and node-filter columns. Transformations that change the source
     * table's {@link Table#getDefinition() definition}, e.g. {@link Table#dropColumns drop columns}, are not supported.
     * This is intended for use in applying {@link Table#sort sorts} and {@link Table#where filters}.
     * 
     * @param sourceTransformer The source transformation to apply
     * @return The new TreeTable
     */
    TreeTable reapply(@NotNull UnaryOperator<Table> sourceTransformer);

    /**
     * Adapt a {@code source} {@link Table} to be used for a {@link Table#tree(String, String) tree} to ensure that the
     * result will have no orphaned nodes. Nodes whose parents do not exist will become children of the root node in the
     * resulting tree. The expected usage pattern is:
     * 
     * <pre>
     * TreeTable result = promoteOrphans(source, idColumn, parentColumn).tree(idColumn, parentColumn)
     * </pre>
     * 
     * @param source The source {@link Table}
     * @param idColumn The name of a column containing a unique identifier for a particular row in the table
     * @param parentColumn The name of a column containing the parent's identifier, {@code null} for rows that are part
     *        of the root table
     * @return A {@link Table} derived from {@code source} that has {@code null} as the parent for any nodes that would
     *         have been orphaned by a call to {@code source.tree(idColumn, parentColumn)}
     */
    static Table promoteOrphans(
            @NotNull final Table source,
            @NotNull final String idColumn,
            @NotNull final String parentColumn) {
        final ColumnName parent = ColumnName.of(parentColumn);
        final ColumnName identifier = ColumnName.of(idColumn);
        final ColumnName sentinel = ColumnName.of("__MATCHED_PARENT_IDENTIFIER__");
        return LivenessScopeStack.computeEnclosed(
                () -> source
                        .naturalJoin(source,
                                List.of(JoinMatch.of(parent, identifier)),
                                List.of(JoinAddition.of(sentinel, identifier)))
                        .updateView(Selectable.of(parent,
                                RawString.of("isNull(" + sentinel.name() + ") ? null : " + parent.name()))),
                source::isRefreshing,
                DynamicNode::isRefreshing);
    }
}
