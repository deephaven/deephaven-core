package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.*;

/**
 * This class provides data indexes for merged tables. It is responsible for ensuring that the provided table accounts
 * for the relative positions of individual table locations in the provided table of indices.
 *
 * <p>
 * This also attempts to defer any actual disk accesses until they are absolutely necessary.
 *
 * @implNote This is an experimental feature, it is likely to change.
 */
@InternalUseOnly
public class StorageBackedDataIndexImpl extends AbstractDataIndex {
    private static final String OFFSET_KEY_COL_NAME = "dh_offset_key";

    @NotNull
    private final WeakHashMap<ColumnSource<?>, String> keyColumnMap;

    private final ColumnSourceManager columnSourceManager;

    private final Table sourceTable;

    @NotNull
    final String[] keyColumnNames;

    /** The table containing the index. Consists of sorted key column(s) and an associated RowSet column. */
    private Table indexTable;

    /** Provides fast lookup from keys to positions in the table **/
    private TObjectIntHashMap<Object> cachedPositionMap;

    private PositionLookup cachedPositionLookup;
    private RowSetLookup cachedRowSetLookup;

    public StorageBackedDataIndexImpl(@NotNull final Table sourceTable,
            final ColumnSource<?>[] keySources,
            final ColumnSourceManager columnSourceManager,
            @NotNull final String[] keyColumnNames) {

        this.sourceTable = sourceTable;
        this.columnSourceManager = columnSourceManager;
        this.keyColumnNames = keyColumnNames;

        // Create an in-order reverse lookup map for the key columnn names.
        keyColumnMap = new WeakHashMap<>(keySources.length);
        for (int ii = 0; ii < keySources.length; ii++) {
            keyColumnMap.put(keySources[ii], keyColumnNames[ii]);
        }

        // Store the column source manager for later use.
        final Table locationTable = columnSourceManager.locationTable();

        if (sourceTable.isRefreshing()) {
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

        // We will defer the actual index creation until it is needed.
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

        // Invalidate the index table and cached lookup objects.
        indexTable = null;
        cachedPositionMap = null;
        cachedPositionLookup = null;
        cachedRowSetLookup = null;

        update.added().forAllRowKeys((long key) -> {
            // Add new locations to the map for addition to the data index (when resolved).
            final TableLocation location = locationColumnSource.get(key);
            final long firstKey = RegionedColumnSource.getFirstRowKey(Math.toIntExact(key));

            final LocationState locationState =
                    new LocationState(location, firstKey, keyColumnNames);
            locations.put(location, locationState);
        });

        update.modified().forAllRowKeys((long key) -> {
            // Invalidate the cached index table for the modified location.
            final TableLocation location = locationColumnSource.get(key);
            locations.get(location).cachedIndexTable = null;
        });
    }

    private final LinkedHashMap<TableLocation, LocationState> locations = new LinkedHashMap<>();

    @Override
    public String[] keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    public Map<ColumnSource<?>, String> keyColumnMap() {
        return keyColumnMap;
    }

    @Override
    public String rowSetColumnName() {
        return INDEX_COL_NAME;
    }

    @Override
    public Table table(final boolean usePrev) {
        if (usePrev && isRefreshing()) {
            throw new UnsupportedOperationException(
                    "usePrev==true is not currently supported for refreshing storage-backed data index tables");
        }

        if (indexTable == null) {
            indexTable = QueryPerformanceRecorder
                    .withNugget("Build Storage Backed Data Index [" + String.join(", ", keyColumnNames) + "]", () -> {

                        final Table[] locationIndexes = new Table[locations.size()];
                        int tCount = 0;
                        for (final LocationState ls : locations.values()) {
                            final Table locationIndex = ls.getCachedIndexTable();
                            // If any location is missing a data index, we must bail out because we can't guarantee a
                            // consistent index.
                            if (locationIndex == null) {
                                return null;
                            }

                            locationIndexes[tCount++] = locationIndex;
                        }

                        // Merge all the individual indexes into a single table.
                        final Table merged = TableTools.merge(locationIndexes);

                        // Partition the merged table by the keys
                        final PartitionedTable pt = merged.partitionBy(keyColumnNames);
                        final PartitionedTable transformed = pt.transform(t -> {
                            // Create a new table containing the key columns and a RowSet.
                            Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
                            for (String keyColumnName : keyColumnNames) {
                                columnSourceMap.put(keyColumnName, t.getColumnSource(keyColumnName));
                            }

                            // Build a new row set from the individual row sets (with their offset keys).
                            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                            try (final CloseableIterator<RowSet> rsIt = t.columnIterator(INDEX_COL_NAME);
                                    final CloseableIterator<Long> keyIt = t.columnIterator(OFFSET_KEY_COL_NAME)) {
                                while (rsIt.hasNext()) {
                                    final RowSet rowSet = rsIt.next();
                                    final long offsetKey = keyIt.next();
                                    builder.appendRowSequenceWithOffset(rowSet, offsetKey);
                                }
                            }
                            final RowSet outputRowSet = builder.build();

                            // Create a SingleValueColumnSource for the row set and add it to the column source map.
                            SingleValueColumnSource<RowSet> rowSetColumnSource =
                                    SingleValueColumnSource.getSingleValueColumnSource(RowSet.class);
                            rowSetColumnSource.set(outputRowSet);
                            columnSourceMap.put(INDEX_COL_NAME, rowSetColumnSource);

                            // The result table row set is a single key. We'll use the first key of input table to get
                            // the
                            // correct key values from the key column sources.
                            final WritableRowSet resultRowSet = RowSetFactory.fromKeys(t.getRowSet().firstRowKey());

                            return new QueryTable(resultRowSet.toTracking(), columnSourceMap);
                        });

                        // Flatten the result table to cache all the redirections we just created.
                        final Table mergedOutput = transformed.merge()
                                .sort(keyColumnNames)
                                .select();

                        return mergedOutput;
                    });
        }
        return indexTable;
    }

    @Override
    public RowSetLookup rowSetLookup() {
        if (cachedRowSetLookup != null) {
            return cachedRowSetLookup;
        }

        PositionLookup positionLookup = positionLookup();
        return (Object o) -> {
            final int position = positionLookup.apply(o);
            if (position < 0) {
                return null;
            }
            return (RowSet) indexTable.getColumnSource(INDEX_COL_NAME).get(position);
        };
    }

    @Override
    public @NotNull PositionLookup positionLookup() {
        if (cachedPositionLookup != null) {
            return cachedPositionLookup;
        }

        // TODO: Does PartitionedTable expose an AggregationProcessor#getRowLookup interface?

        // Resolve the table and decide whether to create a map or use a binary search strategy
        final Table indexTable = table();
        if (indexTable.size() >= BIN_SEARCH_THRESHOLD) {
            // Use a binary search strategy rather than consume memory for the hashmap.
            cachedPositionLookup = buildPositionLookup(indexTable, keyColumnNames);
        } else {
            // Build or use the hashmap
            if (cachedPositionMap == null) {
                cachedPositionMap = buildPositionMap(indexTable, keyColumnNames);
            }
            cachedPositionLookup = cachedPositionMap::get;
        }
        return cachedPositionLookup;
    }

    @Override
    public boolean isRefreshing() {
        return false;
    }

    @Override
    public Table baseTable() {
        return columnSourceManager.locationTable();
    }

    private static class LocationState {
        private final TableLocation location;
        private final long offsetKey;
        private final String[] keyColumns;
        private SoftReference<Table> cachedIndexTable;

        private LocationState(final TableLocation location,
                final long offsetKey,
                @NotNull final String... keyColumns) {
            this.location = location;
            this.offsetKey = offsetKey;
            this.keyColumns = keyColumns;
        }

        @Nullable
        private Table getCachedIndexTable() {
            // Already cached?
            if (cachedIndexTable != null) {
                final Table result = cachedIndexTable.get();
                if (result != null) {
                    return result;
                }
            }

            synchronized (this) {
                if (cachedIndexTable != null) {
                    final Table result = cachedIndexTable.get();
                    if (result != null) {
                        return result;
                    }
                }

                Table indexTable = location.getDataIndex(keyColumns);
                if (indexTable != null) {
                    Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>(indexTable.getColumnSourceMap());

                    // Record the first key as a column of this table using a SingleValueColumnSource.
                    SingleValueColumnSource<?> offsetKeySource =
                            SingleValueColumnSource.getSingleValueColumnSource(long.class);
                    offsetKeySource.set(offsetKey);
                    columnSourceMap.put(OFFSET_KEY_COL_NAME, offsetKeySource);

                    indexTable = new QueryTable(indexTable.getRowSet(), columnSourceMap);
                    cachedIndexTable = new SoftReference<>(indexTable);
                }
                return indexTable;
            }
        }
    }
    // endregion
}
