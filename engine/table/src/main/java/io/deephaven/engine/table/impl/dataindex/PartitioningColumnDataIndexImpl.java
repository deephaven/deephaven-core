package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceManager;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * This data index is from a grouping column, one that contains
 */
public class PartitioningColumnDataIndexImpl extends AbstractDataIndex {
    @NotNull
    private final RegionedColumnSource<?> keySource;

    @NotNull
    private final String keyColumnName;

    private final ColumnSourceManager columnSourceManager;

    private final QueryTable sourceTable;

    /** The table containing the index. Consists of sorted key column(s) and an associated RowSet column. */
    private final Table indexTable;
    private WritableColumnSource<Object> indexKeySource;
    private ObjectArraySource<RowSet> indexRowSetSource;

    /** Provides fast lookup from keys to positions in the table **/
    private TObjectIntHashMap<Object> cachedPositionMap;

    public PartitioningColumnDataIndexImpl(@NotNull final QueryTable sourceTable,
                                           @NotNull final List<ColumnSource<?>> keySources) {
        Assert.eq(keySources.size(), "keySources.size()", 1, "1");
        Assert.eqTrue(keySources.get(0) instanceof RegionedColumnSource, "keySources.get(0) instanceof RegionedColumnSource");

        this.keySource = (RegionedColumnSource<?>)keySources.get(0);

        String matchedColumnName = null;
        // Find the column name in the source table and add to the map.
        for (final Map.Entry<String, ? extends ColumnSource<?>> entry : sourceTable.getColumnSourceMap().entrySet()) {
            if (keySource == entry.getValue()) {
                matchedColumnName = entry.getKey();
                break;
            }
        }
        Assert.eqTrue(matchedColumnName != null, "key column source was not found in the source table");

        keyColumnName = matchedColumnName;

        // Store the column source manager for later use.
        columnSourceManager = ((RegionedColumnSource)keySources.get(0)).getColumnSourceManager();

        this.sourceTable = sourceTable;

        // Build the index table and the position lookup map.
        final Table locationTable = columnSourceManager.locationTable();
        indexKeySource = (WritableColumnSource<Object>) ArrayBackedColumnSource.getMemoryColumnSource(10, keySource.getType(), null);
        indexRowSetSource = (ObjectArraySource<RowSet>)ArrayBackedColumnSource.getMemoryColumnSource(10, RowSet.class, null);

        // Iterate through the location table.
        cachedPositionMap = new TObjectIntHashMap<>(locationTable.intSize(), 0.5F, -1);

        try (final CloseableIterator<TableLocation> locationIt = locationTable.columnIterator(columnSourceManager.locationColumnName());
             final CloseableIterator<Long> offsetIt = locationTable.columnIterator(columnSourceManager.offsetColumnName());
             final CloseableIterator<RowSet> rowSetIt = locationTable.columnIterator(columnSourceManager.rowSetColumnName())) {
            MutableInt position = new MutableInt(0);

            while (locationIt.hasNext()) {
                final TableLocation location = locationIt.next();
                final long offset = offsetIt.next();
                final RowSet shiftedRowSet = rowSetIt.next().shift(offset);

                final Object key = location.getKey().getPartitionValue(keyColumnName);
                final Class<?> clazz = key.getClass();

                final int pos = cachedPositionMap.get(key);
                if (pos == -1) {
                    // Key not found, add it.
                    final int index = position.getAndIncrement();

                    indexKeySource.ensureCapacity(index + 1);
                    if (clazz == Byte.class) {
                        indexKeySource.set(index, (byte)key);
                    } else if (clazz == Character.class) {
                        indexKeySource.set(index, (char)key);
                    } else if (clazz == Float.class) {
                        indexKeySource.set(index, (float)key);
                    } else if (clazz == Double.class) {
                        indexKeySource.set(index, (double)key);
                    } else if (clazz == Short.class) {
                        indexKeySource.set(index, (short)key);
                    } else if (clazz == Integer.class) {
                        indexKeySource.set(index, (int)key);
                    } else if (clazz == Long.class) {
                        indexKeySource.set(index, (long)key);
                    } else {
                        indexKeySource.set(index, key);
                    }

                    indexRowSetSource.ensureCapacity(index + 1);
                    indexRowSetSource.set(index, shiftedRowSet);

                    cachedPositionMap.put(key, index);
                } else {
                    // Key found, update it.
                    final RowSet existingRowSet = indexRowSetSource.get(pos);
                    indexRowSetSource.set(pos, existingRowSet.union(shiftedRowSet));
                }

                position.increment();
            }
            indexTable = new QueryTable(RowSetFactory.flat(position.getValue()).toTracking(), Map.of(
                    keyColumnName, indexKeySource,
                    INDEX_COL_NAME, indexRowSetSource
            ));
        }
        if (sourceTable.isRefreshing()) {
            indexTable.setRefreshing(true);
            indexKeySource.startTrackingPrevValues();
            indexRowSetSource.startTrackingPrevValues();
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
    public @Nullable Table table() {
        return indexTable;
    }

    @Override
    public @Nullable Table prevTable() {
        if (!isRefreshing()) {
            // This index is static, so prev==current
            return table();
        }
        // Return a table containing the previous values of the index table.
        final TrackingRowSet prevRowSet = indexTable.getRowSet().copyPrev().toTracking();
        final Map<String, ColumnSource<?>> prevColumnSourceMap = new LinkedHashMap<>();
        indexTable.getColumnSourceMap().forEach((columnName, columnSource) -> {
            prevColumnSourceMap.put(columnName, columnSource.getPrevSource());
        });

        final Table prevTable = new QueryTable(prevRowSet, prevColumnSourceMap);
        return prevTable;
    }

    @Override
    public @Nullable RowSetLookup rowSetLookup() {
        return (Object o) -> {
            final int position = cachedPositionMap.get(o);
            return position == -1 ? null : indexRowSetSource.get(position);
        };
    }

    @Override
    public @NotNull PositionLookup positionLookup() {
        return cachedPositionMap::get;
    }

    @Override
    public boolean isRefreshing() {
        return indexTable.isRefreshing();
    }
}

