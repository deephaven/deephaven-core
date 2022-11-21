package io.deephaven.engine.table.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.AttributeMap;
import io.deephaven.engine.table.GridAttributes;
import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseable;
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

    /**
     * Opaque interface for objects used to cache snapshot state across multiple invocations of
     * {@link #fillSnapshotChunks(SnapshotState, Table, BitSet, RowSequence, WritableChunk[])}.
     */
    interface SnapshotState extends SafeCloseable {
    }

    /**
     * Make a re-usable snapshot state.
     */
    SnapshotState makeSnapshotState();

    /**
     * Populate data chunks for a snapshot of this HierarchicalTable.
     *
     * @param snapshotState Snapshot state object used to cache data across invocations
     * @param keyTable Type-specific "key" table specifying expanded and contracted nodes
     * @param columns Optional bit-set of columns to include, {@code null} to include all columns
     * @param rows Position-space rows to include from the expanded data specified by {@code keyTable}
     * @param destinations The destination {@link WritableChunk chunks}
     * @return The total expanded data size or an estimate thereof
     */
    long fillSnapshotChunks(
            @NotNull SnapshotState snapshotState,
            @NotNull Table keyTable,
            @NotNull ColumnName keyTableExpandDescendantsColumn,
            @Nullable BitSet columns,
            @NotNull RowSequence rows,
            @NotNull WritableChunk<? extends Values>[] destinations);
}
