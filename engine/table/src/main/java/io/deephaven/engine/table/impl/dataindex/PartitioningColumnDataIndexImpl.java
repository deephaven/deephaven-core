package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * This data index is from a grouping column, one that contains
 */
public class PartitioningColumnDataIndexImpl extends AbstractDataIndex {
    @NotNull
    private final ColumnSource<?> keySource;

    @NotNull
    private final String keyColumnName;

    private final ColumnSourceManager columnSourceManager;

    private final Table sourceTable;

    /** The table containing the index. Consists of sorted key column(s) and an associated RowSet column. */
    private final QueryTable indexTable;
    private final WritableColumnSource<Object> indexKeySource;
    private final ObjectArraySource<RowSet> indexRowSetSource;

    /** Provides fast lookup from keys to positions in the index table **/
    private final TObjectIntHashMap<Object> cachedPositionMap;

    public PartitioningColumnDataIndexImpl(@NotNull final Table sourceTable,
            final ColumnSource<?> keySource,
            final ColumnSourceManager columnSourceManager,
            @NotNull final String keyColumnName) {
        Assert.eqTrue(sourceTable.hasColumns(keyColumnName), keyColumnName + " was not found in the source table");

        this.sourceTable = sourceTable;
        this.keySource = keySource;
        this.columnSourceManager = columnSourceManager;
        this.keyColumnName = keyColumnName;

        // Build the index table and the position lookup map.
        final Table locationTable = columnSourceManager.locationTable();
        indexKeySource = (WritableColumnSource<Object>) ArrayBackedColumnSource.getMemoryColumnSource(
                10,
                keySource.getType(),
                null);
        indexRowSetSource = new ObjectArraySource<>(RowSet.class, null);

        cachedPositionMap = new TObjectIntHashMap<>(locationTable.intSize(), 0.5F, -1);

        indexTable = new QueryTable(RowSetFactory.empty().toTracking(), Map.of(
                this.keyColumnName, indexKeySource,
                INDEX_COL_NAME, indexRowSetSource));

        if (sourceTable.isRefreshing()) {
            indexTable.setRefreshing(true);
            indexKeySource.startTrackingPrevValues();
            indexRowSetSource.startTrackingPrevValues();

            final TableUpdateListener validatorTableListener =
                    new InstrumentedTableUpdateListenerAdapter(locationTable, false) {
                        @Override
                        public void onUpdate(TableUpdate upstream) {
                            processUpdate(upstream, false);
                        }
                    };
            locationTable.addUpdateListener(validatorTableListener);
        }

        // Create a dummy update for the initial state.
        TableUpdate initialUpdate = new TableUpdateImpl(
                locationTable.getRowSet().copy(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);
        processUpdate(initialUpdate, true);
    }

    private synchronized void processUpdate(final TableUpdate update, final boolean initializing) {
        if (update.removed().isNonempty()) {
            throw new UnsupportedOperationException("Removed rows are not currently supported.");
        }

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
                final RowSet shiftedRowSet = rowSetColumnSource.get(key).shift(firstKey);

                final Object locationKey = location.getKey().getPartitionValue(this.keyColumnName);

                final int pos = cachedPositionMap.get(locationKey);
                if (pos == -1) {
                    // Key not found, add it.
                    addEntry(position.getAndIncrement(), locationKey, shiftedRowSet);
                } else {
                    // Key found, update it.
                    final RowSet existingRowSet = indexRowSetSource.get(pos);
                    updateEntry(pos, existingRowSet.union(shiftedRowSet));
                }
            });
        }

        update.modified().forAllRowKeys((long key) -> {
            final TableLocation location = locationColumnSource.get(key);
            final long firstKey = RegionedColumnSource.getFirstRowKey(Math.toIntExact(key));
            final RowSet shiftedRowSet = rowSetColumnSource.get(key).shift(firstKey);

            final Object locationKey = location.getKey().getPartitionValue(this.keyColumnName);

            final int pos = cachedPositionMap.get(locationKey);
            if (pos == -1) {
                // Key not found. This is a problem.
                throw new IllegalStateException("Modified partition key does not exist.");
            } else {
                // Key found, union these rows to the existing set.
                final RowSet existingRowSet = indexRowSetSource.get(pos);
                updateEntry(pos, existingRowSet.union(shiftedRowSet));
            }
        });
    }

    private synchronized void addEntry(final int index, final Object key, final RowSet rowSet) {
        final Class<?> clazz = key.getClass();

        if (clazz == Byte.class) {
            indexKeySource.set(index, (byte) key);
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

        cachedPositionMap.put(key, index);
    }

    private synchronized void updateEntry(final int index, final RowSet rowSet) {
        indexRowSetSource.set(index, rowSet);
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
    public @Nullable Table table(final boolean usePrev) {
        if (usePrev && isRefreshing()) {
            // TODO: need a custom ColumnSource<RowSet> wrapper that returns {@link TrackingRowSet#prev()} for
            // {@link #getPrev(long)}.
            throw new UnsupportedOperationException(
                    "usePrev==true is not currently supported for refreshing partition data index tables");
        }

        // if (usePrev && isRefreshing()) {
        // // Return a table containing the previous values of the index table.
        // final TrackingRowSet prevRowSet = indexTable.getRowSet().copyPrev().toTracking();
        // final Map<String, ColumnSource<?>> prevColumnSourceMap = new LinkedHashMap<>();
        // indexTable.getColumnSourceMap().forEach((columnName, columnSource) -> {
        // prevColumnSourceMap.put(columnName, columnSource.getPrevSource());
        // });
        //
        // final Table prevTable = new QueryTable(prevRowSet, prevColumnSourceMap);
        // return prevTable;
        // }
        return indexTable;
    }

    @Override
    public @Nullable RowSetLookup rowSetLookup(final boolean usePrev) {
        if (usePrev && isRefreshing()) {
            throw new UnsupportedOperationException(
                    "usePrev==true is not currently supported for refreshing partition data index tables");
        }
        return (Object o) -> {
            final int position = cachedPositionMap.get(o);
            return position == -1 ? null : indexRowSetSource.get(position);
        };
    }

    @Override
    public @NotNull PositionLookup positionLookup(final boolean usePrev) {
        if (usePrev && isRefreshing()) {
            throw new UnsupportedOperationException(
                    "usePrev==true is not currently supported for refreshing partition data index tables");
        }
        return cachedPositionMap::get;
    }

    @Override
    public boolean isRefreshing() {
        return indexTable.isRefreshing();
    }

    @Override
    public Table baseIndexTable() {
        return indexTable;
    }

    @Override
    public boolean validate() {
        return true;
    }
}

