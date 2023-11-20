package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * DataIndex over a partitioning column of a SourceTable.
 */
public class PartitioningColumnDataIndexImpl extends BaseDataIndex {

    private final ColumnSource<?> keySource;
    private final String keyColumnName;
    private final ColumnSourceManager columnSourceManager;

    /** The table containing the index. Consists of sorted key column(s) and an associated RowSet column. */
    private final QueryTable indexTable;
    private final WritableColumnSource<?> indexKeySource;
    private final ObjectArraySource<WritableRowSet> indexRowSetSource;
    private final ModifiedColumnSet rowSetModifiedColumnSet;

    /** Provides fast lookup from keys to positions in the index table **/
    private final TObjectIntHashMap<Object> keyPositionMap;

    public PartitioningColumnDataIndexImpl(
            @NotNull final ColumnSource<?> keySource,
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
        indexRowSetSource = new ObjectArraySource<>(WritableRowSet.class, null);

        // TODO-RWC/LAB: Should we consider an approach we're we work like the storage-backed, and use FunctionalColumns?
//        indexTable = locationTable.select(
//                String.format("%s = %s.getPartitionValue(%s)", keyColumnName, columnSourceManager.locationColumnName(), keyColumnName),
//                columnSourceManager.rowSetColumnName())
//                .updateView(String.format("%s = %s.shift(%s.getFirstRowKey())", columnSourceManager.rowSetColumnName(), columnSourceManager.rowSetColumnName(), columnSourceManager.locationColumnName()));
//
        keyPositionMap = new TObjectIntHashMap<>(locationTable.intSize(), 0.5F, -1);

        indexTable = new QueryTable(RowSetFactory.empty().toTracking(), Map.of(
                keyColumnName, indexKeySource,
                INDEX_COL_NAME, indexRowSetSource));

        // Create a dummy update for the initial state.
        TableUpdate initialUpdate = new TableUpdateImpl(
                locationTable.getRowSet().copy(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);
        processUpdate(initialUpdate, true);

        if (locationTable.isRefreshing()) {
            indexTable.setRefreshing(true);
            indexKeySource.startTrackingPrevValues();
            indexRowSetSource.startTrackingPrevValues();
            rowSetModifiedColumnSet = indexTable.newModifiedColumnSet(rowSetColumnName());

            final TableUpdateListener validatorTableListener =
                    new InstrumentedTableUpdateListenerAdapter(locationTable, false) {
                        @Override
                        public void onUpdate(TableUpdate upstream) {
                            processUpdate(upstream, false);
                        }
                    };
            locationTable.addUpdateListener(validatorTableListener);
        } else {
            rowSetModifiedColumnSet = null;
        }
    }

    private synchronized void processUpdate(final TableUpdate update, final boolean initializing) {
        if (update.removed().isNonempty()) {
            throw new UnsupportedOperationException("Removed rows are not currently supported.");
        }
        if (update.shifted().nonempty()) {
            throw new UnsupportedOperationException("Shifted rows are not currently supported.");
        }

        final boolean trackUpdates = isRefreshing() && !initializing;

        final RowSetBuilderSequential addedBuilder = trackUpdates ? null : RowSetFactory.builderSequential();
        final RowSetBuilderRandom modifiedBuilder = trackUpdates ? null : RowSetFactory.builderRandom();

        // Get the location table from the RegionedColumnSourceManager.
        final Table locationTable = columnSourceManager.locationTable();

        // Add all the existing locations to the map.
        final ColumnSource<TableLocation> locationColumnSource =
                locationTable.getColumnSource(columnSourceManager.locationColumnName());
        final ColumnSource<RowSet> rowSetColumnSource =
                locationTable.getColumnSource(columnSourceManager.rowSetColumnName());

        if (update.added().isNonempty()) {
            final MutableInt position = new MutableInt(indexTable.intSize());

            final long newSize = position.getValue() + update.added().size();
            indexKeySource.ensureCapacity(newSize);
            indexRowSetSource.ensureCapacity(newSize);

            update.added().forAllRowKeys((long key) -> {
                final TableLocation location = locationColumnSource.get(key);
                // Compute the offset from the key (which is the location region index).
                final long firstKey = RegionedColumnSource.getFirstRowKey(Math.toIntExact(key));
                final WritableRowSet shiftedRowSet = rowSetColumnSource.get(key).shift(firstKey);

                final Object locationKey = location.getKey().getPartitionValue(keyColumnName);

                final int pos = keyPositionMap.get(locationKey);
                if (pos == -1) {
                    // Key not found, add it.
                    final int addedPos = position.getAndIncrement();
                    addEntry(addedPos, locationKey, shiftedRowSet);
                    if (addedBuilder != null) {
                        addedBuilder.appendKey(pos);
                    }
                } else {
                    // Key found, insert these rows to the existing set.
                    final WritableRowSet existingRowSet = indexRowSetSource.get(pos);
                    existingRowSet.insert(shiftedRowSet);
                    if (modifiedBuilder != null) {
                        modifiedBuilder.addKey(pos);
                    }
                }
            });
        }

        update.modified().forAllRowKeys((long key) -> {
            final TableLocation location = locationColumnSource.get(key);
            final long firstKey = RegionedColumnSource.getFirstRowKey(Math.toIntExact(key));
            final WritableRowSet shiftedRowSet = rowSetColumnSource.get(key).shift(firstKey);

            final Object locationKey = location.getKey().getPartitionValue(keyColumnName);

            final int pos = keyPositionMap.get(locationKey);
            if (pos == -1) {
                // Key not found. This is a problem.
                throw new IllegalStateException("Modified partition key does not exist.");
            } else {
                // Key found, insert these rows to the existing set.
                final WritableRowSet existingRowSet = indexRowSetSource.get(pos);
                existingRowSet.insert(shiftedRowSet);
                if (modifiedBuilder != null) {
                    modifiedBuilder.addKey(pos);
                }
            }
        });

        if (trackUpdates) {
            // Send the downstream updates to any listeners of the index table.
            final RowSet added = addedBuilder.build();
            final RowSet modified = modifiedBuilder.build();

            final TableUpdate downstream = new TableUpdateImpl(
                    added,
                    modified,
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    modified.isNonempty() ? rowSetModifiedColumnSet : ModifiedColumnSet.EMPTY);

            if (!downstream.empty()) {
                indexTable.notifyListeners(downstream);
            }
        }
    }

    private synchronized void addEntry(final int index, final Object key, final WritableRowSet rowSet) {
        final Class<?> clazz = key.getClass();

        if (clazz == Byte.class) {
            indexKeySource.set(index, TypeUtils.unbox((Byte) key));
        } else if (clazz == Character.class) {
            indexKeySource.set(index, (char) key);
        } else if (clazz == Float.class) {
            indexKeySource.set(index, (float) key);
        } else if (clazz == Double.class) {
            indexKeySource.set(index, (double) key);
        } else if (clazz == Short.class) {
            indexKeySource.set(index, (short) key);
        } else if (clazz == Integer.class) {
            indexKeySource.set(index, (int) key);
        } else if (clazz == Long.class) {
            indexKeySource.set(index, (long) key);
        } else {
            indexKeySource.set(index, key);
        }
        indexRowSetSource.set(index, rowSet);

        keyPositionMap.put(key, index);
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
    public @Nullable Table table() {
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
