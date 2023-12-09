package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.ColumnSourceManager;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.RowSetColumnSourceWrapper;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * DataIndex over a partitioning column of a SourceTable.
 */
// TODO-RWC: Maybe we want the data indexes for a source table to live inside the CSM for coupling.
// TODO-RWC: Do we need "previous" support on instantiation?
public class RegionedPartitioningColumnDataIndex<KEY_TYPE> extends BaseDataIndex {

    private static final int KEY_NOT_FOUND = -1;

    private final ColumnSource<KEY_TYPE> keySource;
    private final String keyColumnName;
    private final ColumnSourceManager columnSourceManager;

    /** The table containing the index. Consists of a sorted key column and an associated RowSet column. */
    private final QueryTable indexTable;
    private final WritableColumnSource<KEY_TYPE> indexKeySource;
    private final ObjectArraySource<RowSet> indexRowSetSource;

    /** Provides fast lookup from keys to positions in the index table **/
    private final TObjectIntHashMap<KEY_TYPE> keyPositionMap;

    private final ModifiedColumnSet upstreamLocationModified;
    private final ModifiedColumnSet upstreamRowSetModified;
    private final ModifiedColumnSet downstreamRowSetModified;

    public RegionedPartitioningColumnDataIndex(
            @NotNull final ColumnSource<KEY_TYPE> keySource,
            @NotNull final String keyColumnName,
            @NotNull final ColumnSourceManager columnSourceManager) {
        this.keySource = keySource;
        this.columnSourceManager = columnSourceManager;
        this.keyColumnName = keyColumnName;

        // Build the index table and the position lookup map.
        final QueryTable locationTable = (QueryTable) columnSourceManager.locationTable().coalesce();
        indexKeySource = ArrayBackedColumnSource.getMemoryColumnSource(
                locationTable.size(),
                keySource.getType(),
                keySource.getComponentType());
        indexRowSetSource = new ObjectArraySource<>(RowSet.class, null);
        indexTable = new QueryTable(RowSetFactory.empty().toTracking(), Map.of(
                keyColumnName, indexKeySource,
                ROW_SET_COLUMN_NAME, RowSetColumnSourceWrapper.from(indexRowSetSource)));

        keyPositionMap = new TObjectIntHashMap<>(locationTable.intSize(), 0.5F, KEY_NOT_FOUND);

        // Create a dummy update for the initial state.
        final TableUpdate initialUpdate = new TableUpdateImpl(
                locationTable.getRowSet().copy(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);
        try {
            processUpdate(locationTable, initialUpdate, true);
        } finally {
            initialUpdate.release();
        }

        if (locationTable.isRefreshing()) {
            indexTable.setRefreshing(true);
            // No need to track previous values; we mutate the index table's RowSets in-place, and we never move a key.
            upstreamLocationModified = locationTable.newModifiedColumnSet(columnSourceManager.locationColumnName());
            upstreamRowSetModified = locationTable.newModifiedColumnSet(columnSourceManager.rowSetColumnName());
            downstreamRowSetModified = indexTable.newModifiedColumnSet(rowSetColumnName());
            final TableUpdateListener tableListener = new BaseTable.ListenerImpl(String.format(
                    "Partitioning Column Data Index - %s", keyColumnName), locationTable, indexTable) {
                @Override
                public void onUpdate(@NotNull final TableUpdate upstream) {
                    processUpdate(getParent(), upstream, false);
                }
            };
            locationTable.addUpdateListener(tableListener);
            manage(indexTable);
        } else {
            upstreamLocationModified = null;
            upstreamRowSetModified = null;
            downstreamRowSetModified = null;
        }
    }

    private synchronized void processUpdate(
            @NotNull final Table locationTable,
            @NotNull final TableUpdate upstream,
            final boolean initializing) {
        if (upstream.empty()) {
            return;
        }
        if (upstream.removed().isNonempty()) {
            throw new UnsupportedOperationException("Removed rows are not currently supported");
        }
        if (upstream.shifted().nonempty()) {
            throw new UnsupportedOperationException("Shifted rows are not currently supported");
        }
        if (upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(upstreamLocationModified)) {
            throw new UnsupportedOperationException("Modified keys are not currently supported");
        }
        Assert.assertion(initializing || isRefreshing(), "initializing || isRefreshing()");

        final RowSetBuilderSequential addedBuilder = initializing ? null : RowSetFactory.builderSequential();
        final RowSetBuilderRandom modifiedBuilder = initializing ? null : RowSetFactory.builderRandom();

        final ColumnSource<TableLocation> locationColumnSource =
                locationTable.getColumnSource(columnSourceManager.locationColumnName(), TableLocation.class);
        final ColumnSource<RowSet> rowSetColumnSource =
                locationTable.getColumnSource(columnSourceManager.rowSetColumnName(), RowSet.class);

        if (upstream.added().isNonempty()) {
            upstream.added().forAllRowKeys((final long locationRowKey) -> handleKey(
                    locationRowKey, false, locationColumnSource, rowSetColumnSource, addedBuilder, modifiedBuilder));
        }

        if (upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(upstreamRowSetModified)) {
            upstream.modified().forAllRowKeys((final long locationRowKey) -> handleKey(
                    locationRowKey, true, locationColumnSource, rowSetColumnSource, addedBuilder, modifiedBuilder));
        }

        if (initializing) {
            return;
        }

        // Send the downstream updates to any listeners of the index table
        final RowSet added = addedBuilder.build();
        final WritableRowSet modified = modifiedBuilder.build();
        if (added.isEmpty() && modified.isEmpty()) {
            SafeCloseable.closeAll(added, modified);
            return;
        }

        final TableUpdate downstream = new TableUpdateImpl(
                added,
                RowSetFactory.empty(),
                modified,
                RowSetShiftData.EMPTY,
                modified.isNonempty() ? downstreamRowSetModified : ModifiedColumnSet.EMPTY);
        indexTable.notifyListeners(downstream);
    }

    private void handleKey(
            final long locationRowKey,
            final boolean isModify,
            @NotNull final ColumnSource<TableLocation> locationColumnSource,
            @NotNull final ColumnSource<RowSet> rowSetColumnSource,
            @Nullable final RowSetBuilderSequential addedBuilder,
            @Nullable final RowSetBuilderRandom modifiedBuilder) {
        final TableLocation location = locationColumnSource.get(locationRowKey);
        if (location == null) {
            throw new IllegalStateException(String.format("Null location found at location index %d", locationRowKey));
        }
        final RowSet regionRowSet = rowSetColumnSource.get(locationRowKey);
        if (regionRowSet == null) {
            throw new IllegalStateException(String.format("Null row set found at location index %d", locationRowKey));
        }

        final long regionFirstRowKey = RegionedColumnSource.getFirstRowKey(Math.toIntExact(locationRowKey));
        final KEY_TYPE locationKey = location.getKey().getPartitionValue(keyColumnName);
        final int pos = keyPositionMap.get(locationKey);
        if (pos == KEY_NOT_FOUND) {
            if (isModify) {
                throw new IllegalStateException(String.format("Modified partition key %s not found", locationKey));
            }
            final int addedKeyPos = keyPositionMap.size();
            keyPositionMap.put(locationKey, addedKeyPos);

            indexKeySource.ensureCapacity(addedKeyPos + 1);
            indexKeySource.set(addedKeyPos, locationKey);

            indexRowSetSource.ensureCapacity(addedKeyPos + 1);
            indexRowSetSource.set(addedKeyPos, regionRowSet.shift(regionFirstRowKey));

            if (addedBuilder != null) {
                addedBuilder.appendKey(addedKeyPos);
            }
        } else {
            //noinspection DataFlowIssue
            final WritableRowSet existingRowSet = indexRowSetSource.get(pos).writableCast();
            try (final WritableRowSet shiftedRowSet = regionRowSet.shift(regionFirstRowKey)) {
                // We could assert that:
                // 1. an added location is non-overlapping with the key's existing row set
                // 2. a modified location's current row set is a superset of its previous row set
                // 3. a modified location's previous row set is a subset of the key's existing row set
                existingRowSet.insert(shiftedRowSet);
            }

            if (modifiedBuilder != null && pos <= indexTable.getRowSet().lastRowKey()) {
                modifiedBuilder.addKey(pos);
            }
        }
    }

    @Override
    public String[] keyColumnNames() {
        return new String[] {keyColumnName};
    }

    @Override
    public Map<ColumnSource<?>, String> keyColumnMap() {
        return Map.of(keySource, keyColumnName);
    }

    @Override
    public String rowSetColumnName() {
        return ROW_SET_COLUMN_NAME;
    }

    @Override
    @NotNull
    public Table table() {
        return indexTable;
    }

    @Override
    @NotNull
    public RowSetLookup rowSetLookup() {
        final ColumnSource<RowSet> rowSetColumnSource = rowSetColumn();
        return (final Object key, final boolean usePrev) -> {
            // Pass the object to the position map, then return the row set at that position
            final int position = keyPositionMap.get(key);
            if (position == KEY_NOT_FOUND) {
                return null;
            }
            // The position is *also* the row key in the index table
            return usePrev
                    ? rowSetColumnSource.getPrev(position)
                    : rowSetColumnSource.get(position);
        };
    }

    @Override
    @NotNull
    public PositionLookup positionLookup() {
        return (final Object key, final boolean usePrev) -> keyPositionMap.get(key);
    }

    @Override
    public boolean isRefreshing() {
        return indexTable.isRefreshing();
    }

    @Override
    public boolean validate() {
        return true;
    }
}
