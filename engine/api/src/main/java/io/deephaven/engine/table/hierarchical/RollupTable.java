package io.deephaven.engine.table.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Interface for the result of {@link Table#rollup rollup} operations.
 */
public interface RollupTable extends HierarchicalTable<RollupTable> {

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
     * @return The leaf node type
     */
    @FinalDefault
    default NodeType getLeafNodeType() {
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
     * Get the {@link TableDefinition} that should be exposed to node table consumers, e.g. UI-driven snapshots. This
     * excludes "internal" columns used to organize the rollup or support operations, but includes formatting columns
     * (and the {@link #getKeyWidthColumn() key-width column} for {@link NodeType#Aggregated aggregated} nodes).
     *
     * @param nodeType The {@link NodeType node type} to get the {@link TableDefinition} for
     * @return The externally-visible node {@link TableDefinition} for the requested {@code nodeType}
     * @throws IllegalArgumentException If {@link NodeType#Constituent Constituent} is specified when
     *         {@code includesConstituents() == false}
     */
    TableDefinition getNodeDefinition(@NotNull NodeType nodeType);

    /**
     * Get the {@link Pair input/output column name pairs} from input (source) column names to output (rollup
     * aggregation) column names.
     *
     * @return The input/output column name pairs
     */
    Collection<? extends Pair> getColumnPairs();

    /**
     * Apply a set of filters to the group-by columns of this RollupTable in order to produce a new RollupTable.
     *
     * @param filters The filters to apply; must only reference the group-by columns and must not use column arrays
     * @return The new RollupTable
     */
    RollupTable withFilters(@NotNull Collection<? extends Filter> filters);

    /**
     * Recorder for node-level operations to be applied when gathering snapshots.
     */
    interface NodeOperationsRecorder extends
            FormatOperationsRecorder<NodeOperationsRecorder>,
            SortOperationsRecorder<NodeOperationsRecorder> {
    }

    /**
     * Get a {@link NodeOperationsRecorder recorder} for per-node operations to apply during snapshots of the requested
     * {@link NodeType}.
     *
     * @param nodeType The {@link NodeType node type} that operations will be applied to
     * @return The new recorder
     * @throws IllegalArgumentException If {@link NodeType#Constituent Constituent} is specified when
     *         {@code includesConstituents() == false}
     */
    NodeOperationsRecorder makeNodeOperationsRecorder(@NotNull NodeType nodeType);

    /**
     * Get a new RollupTable that will apply the {@link NodeOperationsRecorder recorded} operations to nodes when
     * gathering snapshots. Operations will be applied only to the {@link NodeType node type} supplied when the elements
     * of {@code nodeOperations} were initially {@link #makeNodeOperationsRecorder(NodeType) created}.
     *
     * @param nodeOperations The node-level operations to apply. Elements must have been initially supplied by
     *        {@link #makeNodeOperationsRecorder(NodeType)} from {@code this} RollupTable.
     * @return The new RollupTable
     */
    RollupTable withNodeOperations(@NotNull NodeOperationsRecorder... nodeOperations);
}
