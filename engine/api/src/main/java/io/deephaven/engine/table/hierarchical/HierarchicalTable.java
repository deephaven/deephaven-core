package io.deephaven.engine.table.hierarchical;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.AttributeMap;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.GridAttributes;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;

/**
 * Base interface for the results of operations that produce a hierarchy of table nodes.
 */
public interface HierarchicalTable<IFACE_TYPE extends HierarchicalTable<IFACE_TYPE>>
        extends AttributeMap<IFACE_TYPE>, GridAttributes<IFACE_TYPE> {

    /**
     * Get the source {@link Table} that was aggregated to make this HierarchicalTable.
     *
     * @return The source table
     */
    Table getSource();

    /**
     * Get the root {@link Table} that represents the top level of this HierarchicalTable.
     *
     * @return The root table
     */
    Table getRoot();

    Context makeSnapshotContext(@NotNull Table keyTable, @Nullable LivenessManager manager);

    /**
     * Populate data chunks for a snapshot of this HierarchicalTable.
     *
     * @param keyTable Type-specific "key" table specifying expanded and contracted nodes
     * @param columns Optional bit-set of columns to include, {@code null} to include all columns
     * @param rows Position-space rows to include from the expanded data specified by {@code keyTable}
     * @param destinations The destination {@link WritableChunk chunks}
     * @return The total expanded data size or an estimate thereof
     */
    long fillSnapshotChunks(
            @NotNull Table keyTable,
            @Nullable BitSet columns,
            @NotNull RowSequence rows,
            @NotNull WritableChunk<? extends Values>[] destinations);
}
