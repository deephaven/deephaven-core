package io.deephaven.engine.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Pair;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.UnaryOperator;

/**
 * Interface for the result of {@link Table#rollup rollup} operations.
 */
public interface RollupTable extends AttributeMap<RollupTable>, GridAttributes<RollupTable> {

    /**
     * Get the source {@link Table} that was aggregated to make this rollup.
     *
     * @return The source table
     */
    Table getSource();

    /**
     * Get the root {@link Table} that represents the top level of this rollup.
     *
     * @return The root table
     */
    Table getRoot();

    /**
     * Get the aggregations for the {@link Table#rollup rollup} operation.
     *
     * @return The rollup aggregations
     */
    Collection<? extends Aggregation> getAggregations();

    /**
     * Are the source table's constituent rows included at the lowest level of the rollup?
     *
     * @return Whether constituents are included
     */
    boolean includesConstituents();

    /**
     * Node types for a rollup.
     */
    enum NodeType {

        /**
         * Nodes at one of the aggregated (rolled up) levels in the RollupTable. These nodes have column names and types
         * that result from the {@link #getAggregations() aggregations}.
         */
        Aggregated,

        /**
         * Nodes at the leaf level when {@link #includesConstituents()} is set. These nodes have column names and types
         * from the {@link #getSource() source} table.
         */
        Constituent
    }

    /**
     * Get the {@link NodeType} at this RollupTable's leaf level.
     *
     * @return The leaf type
     */
    @FinalDefault
    default NodeType getLeafType() {
        return includesConstituents() ? NodeType.Constituent : NodeType.Aggregated;
    }

    /**
     * Get the group-by columns for the {@link Table#rollup rollup} operation.
     *
     * @return The group-by columns
     */
    Collection<? extends ColumnName> getGroupByColumns();

    /**
     * Get the name for a column of integers that supplies the valid width for key rows in a snapshot result. This
     * column allows snapshot consumers to differentiate between {@code null} values in group-by columns that do or do
     * not contribute to a row's keys.
     * 
     * @return The key width column name
     */
    ColumnName getKeyWidthColumn();

    /**
     * Get the name for the "rollup" column that holds the next level node tables in each level of a rollup.
     *
     * @return The rollup column name
     */
    ColumnName getRollupColumn();

    /**
     * Get the {@link Pair input/output column name pairs} from input (source) column names to output (rollup
     * aggregation) column names.
     *
     * @return The input/output column name pairs
     */
    Collection<? extends Pair> getColumnPairs();

    /**
     * Get a {@link NodeOperationsRecorder recorder} for per-node operations to apply during snapshots.
     *
     * @return The new recorder
     */
    NodeOperationsRecorder makeNodeOperationsRecorder();

    /**
     * Get a new RollupTable that will apply the {@link NodeOperationsRecorder recorded} operations to node snapshots.
     *
     * @param nodeTypes The node types to apply {@code nodeOperations} to
     * @param nodeOperations The node-level operations to apply
     * @return The new RollupTable with the recorded node operations applied
     */
    RollupTable withNodeOperations(
            @NotNull Collection<NodeType> nodeTypes,
            @NotNull NodeOperationsRecorder nodeOperations);

    /**
     * Get a new RollupTable with {@code columns} designated for flat filtering. This means that UI-driven filters on
     * those columns will be applied to the nodes during snapshots. If no node-filter columns are designated, all
     * filters will be handled in the default way (by filtering the source table and re-applying the rollup operation to
     * the result).
     *
     * @param columns The columns to designate
     * @return The new RollupTable
     */
    RollupTable withNodeFilterColumns(@NotNull Collection<? extends ColumnName> columns);

    /**
     * Apply a transformation to the source table, e.g. for filtering, and re-apply the rollup operation to produce a
     * new RollupTable.
     * 
     * @param sourceTransformer The source transformation to apply
     * @return The new RollupTable
     */
    RollupTable reapply(@NotNull UnaryOperator<Table> sourceTransformer);
}
