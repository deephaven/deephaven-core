//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.dataindex.AbstractDataIndex;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.RowSetColumnSourceWrapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * DataIndex over a partitioning column of a {@link Table} backed by a {@link RegionedColumnSourceManager}.
 */
class PartitioningColumnDataIndex<KEY_TYPE> extends AbstractDataIndex {

    private static final int KEY_NOT_FOUND = (int) RowSequence.NULL_ROW_KEY;

    private final String keyColumnName;

    private final Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn;

    /** The table containing the index. Consists of a sorted key column and an associated RowSet column. */
    private final QueryTable indexTable;
    private final WritableColumnSource<KEY_TYPE> indexKeySource;
    private final ObjectArraySource<RowSet> indexRowSetSource;

    private final ColumnSource<KEY_TYPE> locationTableKeySource;
    private final ColumnSource<?> locationTableKeySourceReinterpreted;
    private final ColumnSource<RowSet> locationTableRowSetSource;


    /** Provides fast lookup from keys to positions in the index table **/
    private final TObjectIntHashMap<Object> keyPositionMap;

    private final ModifiedColumnSet upstreamKeyModified;
    private final ModifiedColumnSet upstreamRowSetModified;
    private final ModifiedColumnSet downstreamRowSetModified;

    /**
     * Construct a new PartitioningColumnDataIndex. Note that this must be constructed by the
     * {@link RegionedColumnSourceManager} at a time when there cannot be any concurrent "refresh" behavior, and so we
     * can safely use the {@link RegionedColumnSourceManager#locationTable() location table} without snapshotting or
     * considering previous values.
     *
     * @param keyColumnName The key column name
     * @param keySource The key source in the indexed table
     * @param columnSourceManager The column source manager that provides locations and region indexes
     */
    PartitioningColumnDataIndex(
            @NotNull final String keyColumnName,
            @NotNull final ColumnSource<KEY_TYPE> keySource,
            @NotNull final RegionedColumnSourceManager columnSourceManager) {
        this.keyColumnName = Require.neqNull(keyColumnName, "keyColumnName");
        Require.neqNull(keySource, "keySource");
        Require.neqNull(columnSourceManager, "columnSourceManager");

        keyColumnNamesByIndexedColumn = Map.of(keySource, keyColumnName);

        // Build the index table and the position lookup map.
        final QueryTable locationTable = (QueryTable) columnSourceManager.locationTable().coalesce();
        indexKeySource = ArrayBackedColumnSource.getMemoryColumnSource(
                locationTable.size(),
                keySource.getType(),
                keySource.getComponentType());
        indexRowSetSource = new ObjectArraySource<>(RowSet.class);
        indexTable = new QueryTable(RowSetFactory.empty().toTracking(), Map.of(
                keyColumnName, indexKeySource,
                ROW_SET_COLUMN_NAME, RowSetColumnSourceWrapper.from(indexRowSetSource)));

        locationTableKeySource = locationTable.getColumnSource(keyColumnName, keySource.getType());
        locationTableKeySourceReinterpreted = ReinterpretUtils.maybeConvertToPrimitive(locationTableKeySource);
        locationTableRowSetSource = locationTable.getColumnSource(columnSourceManager.rowSetColumnName(), RowSet.class);

        keyPositionMap = new TObjectIntHashMap<>(locationTable.intSize(), 0.5F, KEY_NOT_FOUND);

        // Create a dummy update for the initial state.
        final TableUpdate initialUpdate = new TableUpdateImpl(
                locationTable.getRowSet().copy(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);
        try {
            processUpdate(initialUpdate, true);
        } finally {
            initialUpdate.release();
        }

        if (locationTable.isRefreshing()) {
            // No need to track previous values; we mutate the index table's RowSets in-place, and we never move a key.
            indexTable.getRowSet().writableCast().initializePreviousValue();
            upstreamKeyModified = locationTable.newModifiedColumnSet(keyColumnName);
            upstreamRowSetModified = locationTable.newModifiedColumnSet(columnSourceManager.rowSetColumnName());
            downstreamRowSetModified = indexTable.newModifiedColumnSet(rowSetColumnName());
            final TableUpdateListener tableListener = new BaseTable.ListenerImpl(String.format(
                    "Partitioning Column Data Index - %s", keyColumnName), locationTable, indexTable) {
                @Override
                public void onUpdate(@NotNull final TableUpdate upstream) {
                    processUpdate(upstream, false);
                }
            };
            locationTable.addUpdateListener(tableListener);
            manage(indexTable);
        } else {
            upstreamKeyModified = null;
            upstreamRowSetModified = null;
            downstreamRowSetModified = null;
        }
    }

    private synchronized void processUpdate(
            @NotNull final TableUpdate upstream,
            final boolean initializing) {
        if (upstream.empty()) {
            return;
        }
        if (upstream.shifted().nonempty()) {
            throw new UnsupportedOperationException("Shifted locations are not currently supported");
        }
        if (upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(upstreamKeyModified)) {
            throw new UnsupportedOperationException("Modified location keys are not currently supported");
        }
        Assert.assertion(initializing || isRefreshing(), "initializing || isRefreshing()");

        final int previousSize = keyPositionMap.size();
        final RowSetBuilderRandom modifiedPositionBuilder = initializing ? null : RowSetFactory.builderRandom();

        if (upstream.removed().isNonempty()) {
            Assert.eqFalse(initializing, "initializing");
            upstream.removed().forAllRowKeys((final long locationRowKey) -> handleLocation(
                    locationRowKey, ChangeType.REMOVE, modifiedPositionBuilder));
        }
        if (upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(upstreamRowSetModified)) {
            Assert.eqFalse(initializing, "initializing");
            upstream.modified().forAllRowKeys((final long locationRowKey) -> handleLocation(
                    locationRowKey, ChangeType.MODIFY, modifiedPositionBuilder));
        }
        if (upstream.added().isNonempty()) {
            upstream.added().forAllRowKeys((final long locationRowKey) -> handleLocation(
                    locationRowKey, ChangeType.ADD, modifiedPositionBuilder));
        }

        final int newSize = keyPositionMap.size();
        if (previousSize != newSize) {
            indexTable.getRowSet().writableCast().insertRange(previousSize, newSize - 1);
        }

        if (initializing) {
            return;
        }

        final WritableRowSet modified = modifiedPositionBuilder.build();
        if (previousSize == newSize && modified.isEmpty()) {
            modified.close();
            return;
        }

        final RowSetBuilderSequential removedPositionsBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential resurrectedPositionsBuilder = RowSetFactory.builderSequential();
        modified.forAllRowKeys((final long pos) -> {
            final RowSet indexRowSet = indexRowSetSource.get(pos);
            // noinspection DataFlowIssue
            if (indexRowSet.isEmpty()) {
                removedPositionsBuilder.appendKey(pos);
            } else if (indexRowSet.trackingCast().prev().isEmpty()) {
                resurrectedPositionsBuilder.appendKey(pos);
            }
        });

        final WritableRowSet added = RowSetFactory.fromRange(previousSize, newSize - 1);
        final RowSet removed = removedPositionsBuilder.build();
        modified.remove(removed);
        try (final RowSet resurrected = resurrectedPositionsBuilder.build()) {
            added.insert(resurrected);
            modified.remove(resurrected);
        }

        // Send the downstream updates to any listeners of the index table
        final TableUpdate downstream = new TableUpdateImpl(
                added,
                removed,
                modified,
                RowSetShiftData.EMPTY,
                modified.isNonempty() ? downstreamRowSetModified : ModifiedColumnSet.EMPTY);
        indexTable.notifyListeners(downstream);
    }

    private enum ChangeType {
        // @formatter:off
        ADD("Added"),
        REMOVE("Removed"),
        MODIFY("Modified");
        // @formatter:on

        private final String actionLabel;

        ChangeType(@NotNull final String actionLabel) {
            this.actionLabel = actionLabel;
        }
    }

    private void handleLocation(
            final long locationRowKey,
            @NotNull final ChangeType changeType,
            @Nullable final RowSetBuilderRandom modifiedPositionBuilder) {
        final KEY_TYPE locationKey = locationTableKeySource.get(locationRowKey);
        final Object locationKeyReinterpreted = locationTableKeySourceReinterpreted.get(locationRowKey);

        final RowSet currentRegionRowSet = changeType == ChangeType.REMOVE
                ? null
                : locationTableRowSetSource.get(locationRowKey);
        final RowSet previousRegionRowSet = changeType == ChangeType.ADD
                ? null
                : locationTableRowSetSource.getPrev(locationRowKey);

        if (changeType != ChangeType.REMOVE && (currentRegionRowSet == null || currentRegionRowSet.isEmpty())) {
            throw new IllegalStateException(String.format(
                    "%s partition (index=%d, key=%s): Unexpected null or empty current row set",
                    changeType.actionLabel, locationRowKey, locationKey));
        }
        if (changeType != ChangeType.ADD && (previousRegionRowSet == null || previousRegionRowSet.isEmpty())) {
            throw new IllegalStateException(String.format(
                    "%s partition (index=%d, key=%s): Unexpected null or empty previous row set",
                    changeType.actionLabel, locationRowKey, locationKey));
        }

        // Test using the (maybe) reinterpreted key
        final int pos = keyPositionMap.get(locationKeyReinterpreted);

        // Inserting a new bucket
        if (pos == KEY_NOT_FOUND) {
            if (changeType == ChangeType.REMOVE || changeType == ChangeType.MODIFY) {
                throw new IllegalStateException(String.format("%s partition (index=%d, key=%s): Key not found",
                        changeType.actionLabel, locationRowKey, locationKey));
            }
            final int addedKeyPos = keyPositionMap.size();
            // Store the (maybe) reinterpreted key in the lookup hashmap.
            keyPositionMap.put(locationKeyReinterpreted, addedKeyPos);

            // Use the original key for the index table output column.
            indexKeySource.ensureCapacity(addedKeyPos + 1);
            indexKeySource.set(addedKeyPos, locationKey);

            indexRowSetSource.ensureCapacity(addedKeyPos + 1);
            indexRowSetSource.set(addedKeyPos, currentRegionRowSet.copy().toTracking());
            return;
        }

        // Updating an existing bucket
        // noinspection DataFlowIssue
        final WritableRowSet existingRowSet = indexRowSetSource.get(pos).writableCast();
        // We _could_ assert that:
        // 1. An added location is non-overlapping with the key's existing row set
        // 2. A modified location's current row set is a superset of its previous row set (with existing RCSM)
        // 3. A removed or modified location's previous row set is a subset of the key's existing row set
        if (previousRegionRowSet != null) {
            existingRowSet.remove(previousRegionRowSet);
        }
        if (currentRegionRowSet != null) {
            existingRowSet.insert(currentRegionRowSet);
        }
        if (modifiedPositionBuilder != null) {
            // Note that once done processing everything, we're going to adjust this to pull out transitions _from_
            // empty as adds and _to_ empty as removes.
            modifiedPositionBuilder.addKey(pos);
        }
    }

    @Override
    @NotNull
    public List<String> keyColumnNames() {
        return List.of(keyColumnName);
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
        return keyColumnNamesByIndexedColumn;
    }

    @Override
    @NotNull
    public Table table() {
        return indexTable;
    }

    @Override
    @NotNull
    public RowKeyLookup rowKeyLookup() {
        return (final Object key, final boolean usePrev) -> keyPositionMap.get(key);
    }

    @Override
    public boolean isRefreshing() {
        return indexTable.isRefreshing();
    }

    @Override
    public boolean isValid() {
        return true;
    }
}
