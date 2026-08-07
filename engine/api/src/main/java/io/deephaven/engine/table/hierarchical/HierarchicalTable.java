//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;
import java.util.List;

/**
 * Base interface for the results of operations that produce a hierarchy of table nodes.
 */
public interface HierarchicalTable<IFACE_TYPE extends HierarchicalTable<IFACE_TYPE>>
        extends AttributeMap<IFACE_TYPE>, GridAttributes<IFACE_TYPE>, LivenessReferent {

    /**
     * Get a description of this HierarchicalTable.
     *
     * @return The description
     */
    String getDescription();

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
     * Get a re-usable, static key {@link Table} with zero rows that will cause a snapshot to expand only the default
     * nodes.
     *
     * @return An empty key {@link Table} for default expansions
     */
    Table getEmptyExpansionsTable();

    /**
     * Get the name of a column of {@link java.lang.Boolean Booleans} that denotes whether a row is expanded (or
     * expandable). It takes on the value {@code null} for rows that are not expandable, {@code false} for expandable
     * but unexpanded rows, and {@code true} for expanded rows. This column is "synthetic"; that is, it's not part of
     * the data, but rather calculated as part of snapshotting.
     *
     * @return The name of a column that denotes whether a row is expanded or expandable
     */
    ColumnName getRowExpandedColumn();

    /**
     * Get the name of a column that denotes row depth. This column is "synthetic"; that is, it's not part of the data,
     * but rather calculated as part of snapshotting.
     *
     * @return The name of a column that denotes row depth
     */
    ColumnName getRowDepthColumn();

    /**
     * Get the {@link ColumnDefinition definitions} for all structural columns. Structural columns are synthetic columns
     * that allow snapshot consumers to make sense of the relationship between rows, and should always be included in
     * the requested columns set when snapshotting a HierarchicalTable. This list includes the
     * {@link #getRowDepthColumn() row-depth column} and the {@link #getRowExpandedColumn() row-expanded column}, but
     * never includes type-specific node-level columns.
     *
     * @return A list of @link ColumnDefinition definitions} for all structural columns
     */
    List<ColumnDefinition<?>> getStructuralColumnDefinitions();

    /**
     * Get the {@link ColumnDefinition definitions} for all available columns that may be requested in a snapshot.
     * <p>
     * The result will always begin with the {@link #getStructuralColumnDefinitions() structural columns}, which are
     * then followed by type-specific node-level columns.
     *
     * @return A list of {@link ColumnDefinition definitions} for all available columns that may be requested in a
     *         snapshot
     */
    List<ColumnDefinition<?>> getAvailableColumnDefinitions();

    /**
     * Opaque interface for objects used to cache snapshot state across multiple invocations of
     * {@link #snapshot(SnapshotState, Table, ColumnName, BitSet, RowSequence, WritableChunk[])}.
     * <p>
     * Implementations may have limited support for concurrency, meaning that multiple concurrent snapshot calls using
     * the same state may be internally serialized.
     * <p>
     * In order to ensure that a state remains usable, all dependent objects should maintain a liveness reference using
     * {@link LivenessReferent#retainReference()} and {@link LivenessReferent#dropReference()}.
     */
    interface SnapshotState extends LivenessReferent {
    }

    /**
     * Make a re-usable snapshot state. The result will ensure liveness for {@code this} HierarchicalTable if the
     * {@link #getSource() source} {@link Table#isRefreshing() isRefreshing}, and callers must similarly retain the
     * result for the duration of their usage under the same conditions.
     */
    SnapshotState makeSnapshotState();

    /**
     * Key table action value specifying that a node should be expanded. This is the default action if no action column
     * is specified.
     */
    byte KEY_TABLE_ACTION_EXPAND = 0b001;
    /**
     * Key table action value specifying that a node should be expanded, and that its descendants should all be expanded
     * unless they are included in the key table with a {@link #KEY_TABLE_ACTION_CONTRACT contraction} or their parent
     * is included with a {@link #KEY_TABLE_ACTION_EXPAND simple expansion}.
     */
    byte KEY_TABLE_ACTION_EXPAND_ALL = 0b011;
    /**
     * Key table action value specifying that a node should be contracted. The node must descend from a node that was
     * {@link #KEY_TABLE_ACTION_EXPAND_ALL expanded with its descendants} or {@link #expandToDepthAction(int) expanded
     * to a specific depth}, or this key table row will have no effect.
     */
    byte KEY_TABLE_ACTION_CONTRACT = 0b100;

    /**
     * Base value for expand-to-depth actions. The actual depth is encoded as
     * {@code KEY_TABLE_ACTION_EXPAND_TO_DEPTH_BASE + (depth - 1)}, where {@code depth >= 1}. A depth of 1 is equivalent
     * to {@link #KEY_TABLE_ACTION_EXPAND}, and an unlimited depth is equivalent to
     * {@link #KEY_TABLE_ACTION_EXPAND_ALL}.
     * <p>
     * The node will be expanded, and its descendants will be expanded up to {@code depth} levels below the directive
     * node, unless they are included in the key table with a {@link #KEY_TABLE_ACTION_CONTRACT contraction} or their
     * direct parent has a directive with action {@link #KEY_TABLE_ACTION_EXPAND}.
     */
    byte KEY_TABLE_ACTION_EXPAND_TO_DEPTH_BASE = 0b0111_0000;

    /**
     * Create a key table action byte value for expanding a node to a specific depth.
     *
     * @param depth The number of levels to expand below the directive node (must be &gt;= 1 and &lt;= 15)
     * @return The action byte value encoding the expand-to-depth action
     * @throws IllegalArgumentException if depth is out of the valid range
     */
    static byte expandToDepthAction(final int depth) {
        if (depth < 1 || depth > Byte.MAX_VALUE - KEY_TABLE_ACTION_EXPAND_TO_DEPTH_BASE + 1) {
            throw new IllegalArgumentException(
                    "Expand-to-depth must be between 1 and "
                            + (Byte.MAX_VALUE - KEY_TABLE_ACTION_EXPAND_TO_DEPTH_BASE + 1) + ", got " + depth);
        }
        return (byte) (KEY_TABLE_ACTION_EXPAND_TO_DEPTH_BASE + depth - 1);
    }

    /**
     * Take a snapshot of the data in the grid defined by {@code columns}, {@code rows}, and the directives in
     * {@code keyTable}. Accumulate the results in {@code destinations}.
     *
     * @param snapshotState Snapshot state object used to cache data across invocations. Must have been created by this
     *        HierarchicalTable with {@link #makeSnapshotState()}.
     * @param keyTable Type-specific "key" table specifying expanded and contracted nodes
     * @param keyTableActionColumn The name of a column of {@code byte} on {@code keyTable} that specifies whether nodes
     *        should be {@link #KEY_TABLE_ACTION_EXPAND expanded}, {@link #KEY_TABLE_ACTION_EXPAND_ALL expanded with
     *        their descendants}, {@link #expandToDepthAction(int) expanded to a specific depth}, or
     *        {@link #KEY_TABLE_ACTION_CONTRACT contracted}. If {@code null}, all rows are treated as
     *        {@link #KEY_TABLE_ACTION_EXPAND simple expansions}.
     * @param columns Optional bit-set of columns to include, {@code null} to include all columns
     * @param rows Position-space rows to include from the expanded data specified by {@code keyTable}
     * @param destinations The destination {@link WritableChunk chunks}
     * @return The total expanded data size or an estimate thereof
     */
    long snapshot(
            @NotNull SnapshotState snapshotState,
            @NotNull Table keyTable,
            @Nullable ColumnName keyTableActionColumn,
            @Nullable BitSet columns,
            @NotNull RowSequence rows,
            @NotNull WritableChunk<? super Values>[] destinations);
}
