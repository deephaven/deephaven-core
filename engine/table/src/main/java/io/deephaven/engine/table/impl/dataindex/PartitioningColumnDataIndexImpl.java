package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ColumnSourceManager;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.RowSetColumnSourceWrapper;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import org.apache.commons.lang3.mutable.MutableInt;
import org.checkerframework.checker.units.qual.N;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * DataIndex over a partitioning column of a SourceTable.
 */
public class PartitioningColumnDataIndexImpl<KEY_TYPE> extends BaseDataIndex {

    private final ColumnSource<KEY_TYPE> keySource;
    private final String keyColumnName;
    private final ColumnSourceManager columnSourceManager;

    /** The table containing the index. Consists of a sorted key column and an associated RowSet column. */
    private final QueryTable indexTable;
    private final WritableColumnSource<KEY_TYPE> indexKeySource;
    private final ObjectArraySource<RowSet> indexRowSetSource;
    private final ModifiedColumnSet rowSetModifiedColumnSet;

    /** Provides fast lookup from keys to positions in the index table **/
    private final TObjectIntHashMap<Object> keyPositionMap;

    public PartitioningColumnDataIndexImpl(
            @NotNull final ColumnSource<KEY_TYPE> keySource,
            @NotNull final String keyColumnName,
            @NotNull final ColumnSourceManager columnSourceManager) {
        this.keySource = keySource;
        this.columnSourceManager = columnSourceManager;
        this.keyColumnName = keyColumnName;

        // Build the index table and the position lookup map.
        final Table locationTable = columnSourceManager.locationTable();
        indexKeySource = ArrayBackedColumnSource.getMemoryColumnSource(
                locationTable.size(),
                keySource.getType(),
                keySource.getComponentType());
        indexRowSetSource = new ObjectArraySource<>(RowSet.class, null);
        indexTable = new QueryTable(RowSetFactory.empty().toTracking(), Map.of(
                keyColumnName, indexKeySource,
                INDEX_COL_NAME, RowSetColumnSourceWrapper.from(indexRowSetSource)));

        keyPositionMap = new TObjectIntHashMap<>(locationTable.intSize(), 0.5F, -1);

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
            indexTable.setRefreshing(true);
            // No need to track previous values; we mutate the index table's RowSets in-place, and we never move a key.
            rowSetModifiedColumnSet = indexTable.newModifiedColumnSet(rowSetColumnName());
            // TODO-RWC: This should probably just be a `BaseTable.ListenerImpl`.
            final TableUpdateListener tableListener =
                    new InstrumentedTableUpdateListenerAdapter(locationTable, false) {
                        @Override
                        public void onUpdate(@NotNull final TableUpdate upstream) {
                            processUpdate(upstream, false);
                        }
                    };
            locationTable.addUpdateListener(tableListener);
        } else {
            rowSetModifiedColumnSet = null;
        }
    }

    private synchronized void processUpdate(final TableUpdate upstream, final boolean initializing) {
        if (upstream.empty()) {
            return;
        }
        if (upstream.removed().isNonempty()) {
            throw new UnsupportedOperationException("Removed rows are not currently supported.");
        }
        if (upstream.shifted().nonempty()) {
            throw new UnsupportedOperationException("Shifted rows are not currently supported.");
        }

        final boolean generateUpdates = isRefreshing() && !initializing;

        final RowSetBuilderSequential addedBuilder = generateUpdates ? RowSetFactory.builderSequential() : null;
        final RowSetBuilderRandom modifiedBuilder = generateUpdates ? RowSetFactory.builderRandom() : null;

        // Get the location table from the RegionedColumnSourceManager.
        final Table locationTable = columnSourceManager.locationTable();

        // Add all the existing locations to the map.
        final ColumnSource<TableLocation> locationColumnSource =
                locationTable.getColumnSource(columnSourceManager.locationColumnName());
        final ColumnSource<RowSet> rowSetColumnSource =
                locationTable.getColumnSource(columnSourceManager.rowSetColumnName());

        if (upstream.added().isNonempty()) {
            final long newSize = indexTable.size() + upstream.added().size();
            indexKeySource.ensureCapacity(newSize);
            indexRowSetSource.ensureCapacity(newSize);

            upstream.added().forAllRowKeys((long key) -> {
                final TableLocation location = locationColumnSource.get(key);
                // Compute the offset from the key (which is the location region index).
                // TODO-RWC: Maybe we want the data indexes for a source table to live inside the CSM for coupling.
                final long firstKey = RegionedColumnSource.getFirstRowKey(Math.toIntExact(key));
                // TODO-RWC: shiftedRowSet is being leaked in the else
                final WritableRowSet shiftedRowSet = rowSetColumnSource.get(key).shift(firstKey);

                // noinspection DataFlowIssue
                final KEY_TYPE locationKey = location.getKey().getPartitionValue(keyColumnName);

                final int pos = keyPositionMap.get(locationKey);
                if (pos == -1) {
                    // Key not found, add it.
                    final int addedPos = keyPositionMap.size();
                    indexKeySource.set(addedPos, locationKey);
                    indexRowSetSource.set(addedPos, shiftedRowSet);
                    keyPositionMap.put(locationKey, addedPos);

                    if (addedBuilder != null) {
                        addedBuilder.appendKey(pos);
                    }
                } else {
                    // Key found, insert these rows to the existing set.
                    final WritableRowSet existingRowSet = indexRowSetSource.get(pos).writableCast();
                    // We could assert that shiftedRowSet does not overlap existingRowSet.
                    existingRowSet.insert(shiftedRowSet);
                    if (modifiedBuilder != null) {
                        modifiedBuilder.addKey(pos);
                    }
                }
            });
        }

        if (upstream.modified().isNonempty()) {
            upstream.modified().forAllRowKeys((long key) -> {
                final TableLocation location = locationColumnSource.get(key);
                final long firstKey = RegionedColumnSource.getFirstRowKey(Math.toIntExact(key));
                // TODO-RWC: shiftedRowSet is being leaked in the else
                final WritableRowSet shiftedRowSet = rowSetColumnSource.get(key).shift(firstKey);

                //noinspection DataFlowIssue
                final Object locationKey = location.getKey().getPartitionValue(keyColumnName);

                final int pos = keyPositionMap.get(locationKey);
                if (pos == -1) {
                    // Key not found. This is a problem.
                    throw new IllegalStateException("Modified partition key does not exist.");
                } else {
                    // Key found, insert these rows to the existing set.
                    final WritableRowSet existingRowSet = indexRowSetSource.get(pos).writableCast();
                    // We could assert that shiftedRowSet is a superset of existingRowSet.
                    existingRowSet.insert(shiftedRowSet);
                    if (modifiedBuilder != null) {
                        modifiedBuilder.addKey(pos);
                    }
                }
            });
        }

        if (generateUpdates) {
            // Send the downstream updates to any listeners of the index table.
            final RowSet added = addedBuilder.build();
            final RowSet modified = modifiedBuilder.build();

            final TableUpdate downstream = new TableUpdateImpl(
                    added,
                    RowSetFactory.empty(),
                    modified,
                    RowSetShiftData.EMPTY,
                    modified.isNonempty() ? rowSetModifiedColumnSet : ModifiedColumnSet.EMPTY);

            if (!downstream.empty()) {
                indexTable.notifyListeners(downstream);
            }
        }
    }

    @Override
    public String[] keyColumnNames() {
        return new String[0];
    }

    @Override
    public Map<ColumnSource<?>, String> keyColumnMap() {
        return Map.of(keySource, keyColumnName);
    }

    @Override
    public String rowSetColumnName() {
        return INDEX_COL_NAME;
    }

    @Override
    @NotNull
    public Table table() {
        return indexTable;
    }

    @Override
    public @Nullable RowSetLookup rowSetLookup() {
        final ColumnSource<RowSet> rowSetColumnSource = rowSetColumn();
        return (Object key, boolean usePrev) -> {
            // Pass the object to the position map, then return the row set at that position.
            final int position = keyPositionMap.get(key);
            if (position == -1) {
                return null;
            }
            if (usePrev) {
                final long prevRowKey = table().getRowSet().prev().get(position);
                return rowSetColumnSource.getPrev(prevRowKey);
            } else {
                final long rowKey = table().getRowSet().get(position);
                return rowSetColumnSource.get(rowKey);
            }
        };
    }

    @Override
    public @NotNull PositionLookup positionLookup() {
        return (Object key, boolean usePrev) -> keyPositionMap.get(key);
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
