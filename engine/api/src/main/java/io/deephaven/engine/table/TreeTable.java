package io.deephaven.engine.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Pair;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.UnaryOperator;

/**
 * Interface for the result of {@link Table#tree(String, String)} tree} operations.
 */
public interface TreeTable extends AttributeMap<TreeTable>, GridAttributes<TreeTable> {

    /**
     * Get the source {@link Table} that was aggregated to make this tree.
     *
     * @return The source table
     */
    Table getSource();

    /**
     * Get the root {@link Table} that represents the top level of this tree.
     *
     * @return The root table
     */
    Table getRoot();

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
     * on those columns will be applied to the nodes during snapshots. If no node-filter columns are designated, all
     * filters will be handled at node level. If node-filter columns are designated, filters that include other columns
     * will be handled by filtering the source table and re-applying the tree operation to the result to produce a new
     * TreeTable. Users of orphan promotion or other strategies to govern the structure of the tree should consider
     * specifying an empty set of node-filter columns.
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
}
