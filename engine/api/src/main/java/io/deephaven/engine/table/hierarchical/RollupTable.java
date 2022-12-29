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
     * Get the {@link TableDefinition} that should be exposed to node table consumers, e.g. UI-driven snapshots. This
     * excludes "internal" columns used to organize the rollup or support operations, as well as the
     * {@link #getRowDepthColumn() row-depth column} and {@link #getRowExpandedColumn() row-expanded column}, but
     * includes formatting columns.
     *
     * @param nodeType The {@link NodeType node type} to get the {@link TableDefinition} for
     * @return The externally-visible node {@link TableDefinition} for the requested {@code nodeType}
     * @throws IllegalArgumentException If {@link NodeType#Constituent Constituent} is specified when
     *         {@code includesConstituents() == false}
     */
    TableDefinition getNodeDefinition(@NotNull NodeType nodeType);

    /**
     * @inheritDoc
     * @implNote RollupTable implementations of this method have three distinct sets of columns: the "extra" columns,
     *           the aggregated node columns, and the constituent node columns (if and only if constituents are
     *           included), included in the listed order. Constituent node columns will have their names adjusted to
     *           disambiguate them from aggregated node columns, since {@link TableDefinition table definitions} do not
     *           permit name collisions. For example, an implementation might prefix all constituent column names with
     *           {@code "__CONSTITUENT_"}.
     * @return The externally-visible {@link TableDefinition} for snapshots
     */
    TableDefinition getSnapshotDefinition();

    /**
     * Get the {@link Pair input/output column name pairs} from input (source) column names to output (rollup
     * aggregation) column names. These are used in conjunction with the {@link #getSnapshotDefinition() snapshot
     * definition} by snapshot-driven consumers.
     *
     * @implNote The input column names will be adjusted to match the disambiguated constituent column names found in
     *           {@link #getSnapshotDefinition() snapshot definitions} for constituent columns. For example, an
     *           implementation might prefix all input column names with {@code "__CONSTITUENT_"}.
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

        /**
         * Test if this NodeOperationsRecorder has recorded any operations.
         *
         * @return Whether this NodeOperationsRecorder has recorded any operations
         */
        boolean isEmpty();
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

    /**
     * Translate node operations for aggregated nodes to the closest equivalent for a constituent node. May only be
     * invoked if {@code includesConstituents() == true}.
     *
     * @param aggregatedNodeOperationsToTranslate Node-level operations for aggregated nodes, initially supplied by
     *        {@code makeNodeOperationsRecorder(NodeType.Aggregated)}.
     * @return Node-level operations for constituent nodes
     */
    NodeOperationsRecorder translateAggregatedNodeOperationsForConstituentNodes(
            @NotNull NodeOperationsRecorder aggregatedNodeOperationsToTranslate);
}
