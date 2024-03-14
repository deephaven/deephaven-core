//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.hierarchicaltable;

import io.deephaven.api.ColumnName;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;

/**
 * Server-side "view" object representing a client's snapshot target for HierarchicalTable data.
 * <p>
 * Instances associate two different kinds of information to fully describe the view:
 * <ul>
 * <li>The {@link HierarchicalTable} instance</li>
 * <li>The key {@link Table} information to be used when
 * {@link HierarchicalTable#snapshot(HierarchicalTable.SnapshotState, Table, ColumnName, BitSet, RowSequence, WritableChunk[])
 * snapshotting} the {@link HierarchicalTable}</li>
 * </ul>
 * <p>
 * Instances also store re-usable snapshot states across snapshot invocations.
 */
public final class HierarchicalTableView extends LivenessArtifact {

    private final HierarchicalTable<?> hierarchicalTable;
    private final HierarchicalTable.SnapshotState snapshotState;

    private final Table keyTable;
    private final ColumnName keyTableActionColumn;

    private HierarchicalTableView(
            @NotNull final HierarchicalTable<?> hierarchicalTable,
            @NotNull final HierarchicalTable.SnapshotState snapshotState,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn) {
        this.hierarchicalTable = hierarchicalTable;
        this.snapshotState = snapshotState;
        this.keyTable = keyTable;
        this.keyTableActionColumn = keyTableActionColumn;
        if (keyTable.isRefreshing()) {
            manage(keyTable);
        }
        if (hierarchicalTable.getSource().isRefreshing()) {
            // snapshotState is responsible for managing hierarchicalTable, and hierarchicalTable is responsible for
            // managing its source and any other dependencies.
            manage(snapshotState);
        }
    }

    public HierarchicalTable<?> getHierarchicalTable() {
        return hierarchicalTable;
    }

    public HierarchicalTable.SnapshotState getSnapshotState() {
        return snapshotState;
    }

    public Table getKeyTable() {
        return keyTable;
    }

    public ColumnName getKeyTableActionColumn() {
        return keyTableActionColumn;
    }

    /**
     * Make a new HierarchicalTableView that will snapshot {@code hierarchicalTable} according to the expansions
     * described by {@code keyTable}.
     * 
     * @param hierarchicalTable The {@link HierarchicalTable} to snapshot
     * @param keyTable The {@link Table} to use for expansion key data
     * @param keyTableActionColumn The {@link ColumnName} of expansion actions, if other than
     *        {@link HierarchicalTable#KEY_TABLE_ACTION_EXPAND expand}.
     * @return The new HierarchicalTableView, which will have a new {@link HierarchicalTable.SnapshotState snapshot
     *         state}
     */
    public static HierarchicalTableView makeFromHierarchicalTable(
            @NotNull final HierarchicalTable<?> hierarchicalTable,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn) {
        return new HierarchicalTableView(
                hierarchicalTable, hierarchicalTable.makeSnapshotState(), keyTable, keyTableActionColumn);
    }

    /**
     * Make a new HierarchicalTableView that will snapshot {@code hierarchicalTable} with only the root expanded.
     *
     * @param hierarchicalTable The {@link HierarchicalTable} to snapshot
     * @return The new HierarchicalTableView, which will have a new {@link HierarchicalTable.SnapshotState snapshot
     *         state}
     */
    public static HierarchicalTableView makeFromHierarchicalTable(
            @NotNull final HierarchicalTable<?> hierarchicalTable) {
        return new HierarchicalTableView(
                hierarchicalTable, hierarchicalTable.makeSnapshotState(),
                hierarchicalTable.getEmptyExpansionsTable(), null);
    }

    /**
     * Make a new HierarchicalTableView from an existing one, which will snapshot the existing view's
     * {@link HierarchicalTable} according to the expansions described by {@code keyTable}.
     *
     * @param existingView The existing HierarchicalTableView
     * @param keyTable The {@link Table} to use for expansion key data
     * @param keyTableActionColumn The {@link ColumnName} of expansion actions, if other than
     *        {@link HierarchicalTable#KEY_TABLE_ACTION_EXPAND expand}.
     * @return The new HierarchicalTableView, which will share {@link HierarchicalTable.SnapshotState snapshot state}
     *         with {@code existing}
     */
    public static HierarchicalTableView makeFromExistingView(
            @NotNull final HierarchicalTableView existingView,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn) {
        return new HierarchicalTableView(
                existingView.hierarchicalTable, existingView.snapshotState, keyTable, keyTableActionColumn);
    }

    /**
     * Make a new HierarchicalTableView from an existing one, which will snapshot the existing view's
     * {@link HierarchicalTable} with only the root expanded.
     *
     * @param existingView The existing HierarchicalTableView
     * @return The new HierarchicalTableView, which will share {@link HierarchicalTable.SnapshotState snapshot state}
     *         with {@code existing}
     */
    public static HierarchicalTableView makeFromExistingView(
            @NotNull final HierarchicalTableView existingView) {
        return new HierarchicalTableView(
                existingView.hierarchicalTable, existingView.snapshotState,
                existingView.hierarchicalTable.getEmptyExpansionsTable(), null);
    }
}
