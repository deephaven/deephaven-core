package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ColumnSourceManager;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;


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
    private static final String OFFSET_KEY_COL_NAME = "__OffsetKey";

    @NotNull
    private final WeakHashMap<ColumnSource<?>, String> keyColumnMap;

    private final ColumnSourceManager columnSourceManager;

    private final QueryTable sourceTable;

    @NotNull
    final String[] keyColumnNames;

    /** The table containing the index. Consists of sorted key column(s) and an associated RowSet column. */
    private Table indexTable;

    /** Provides fast lookup from keys to positions in the table **/
    private TObjectIntHashMap<Object> cachedPositionMap;

    private PositionLookup cachedPositionLookup;
    private RowSetLookup cachedRowSetLookup;

    public StorageBackedDataIndexImpl(@NotNull final QueryTable sourceTable,
            @NotNull final List<ColumnSource<?>> keySources) {

        // Create an array to hold the key column names from the source table (and replicated into the index table).
        keyColumnNames = new String[keySources.size()];

        // Create an in-order reverse lookup map for the key columnn names.
        keyColumnMap = new WeakHashMap<>(keySources.size());
        for (int ii = 0; ii < keySources.size(); ii++) {
            ColumnSource<?> keySource = keySources.get(ii);

            Assert.eqTrue(keySource instanceof RegionedColumnSource, "keySource instanceof RegionedColumnSource");

            // Find the column name in the source table and add to the map.
            for (final Map.Entry<String, ? extends ColumnSource<?>> entry : sourceTable.getColumnSourceMap()
                    .entrySet()) {
                if (keySource == entry.getValue()) {
                    final String columnName = entry.getKey();
                    keyColumnMap.put(keySource, columnName);
                    keyColumnNames[ii] = columnName;
                    break;
                }
            }
        }

        // Store the column source manager for later use.
        columnSourceManager = ((RegionedColumnSource) keySources.get(0)).getColumnSourceManager();

        this.sourceTable = sourceTable;

        // Get the location table from the Regioned Column Source Manager.
        final Table locationTable = columnSourceManager.locationTable();

        // Add all the existing locations to the map.
        final ColumnSource<TableLocation> locationColumnSource =
                locationTable.getColumnSource(columnSourceManager.locationColumnName());
        final ColumnSource<Long> offsetColumnSource =
                locationTable.getColumnSource(columnSourceManager.offsetColumnName());

        locationTable.getRowSet().forAllRowKeys((long key) -> {
            final TableLocation location = locationColumnSource.get(key);
            final long firstKey = offsetColumnSource.getLong(key);

            final LocationState locationState = new LocationState(location, firstKey, keyColumnNames);
            locations.put(location, locationState);
        });

        if (sourceTable.isRefreshing()) {
            final TableUpdateListener validatorTableListener =
                    new InstrumentedTableUpdateListenerAdapter(locationTable, false) {
                        @Override
                        public void onUpdate(TableUpdate upstream) {
                            synchronized (StorageBackedDataIndexImpl.this) {
                                // Invalidate the index table and cached lookup objects.
                                indexTable = null;
                                cachedPositionMap = null;
                                cachedPositionLookup = null;
                                cachedRowSetLookup = null;

                                upstream.added().forAllRowKeys((long key) -> {
                                    // Add new locations to the map for addition to the data index (when resolved).
                                    final TableLocation location = locationColumnSource.get(key);
                                    final long firstKey = offsetColumnSource.getLong(key);

                                    final LocationState locationState =
                                            new LocationState(location, firstKey, keyColumnNames);
                                    locations.put(location, locationState);
                                });

                                upstream.modified().forAllRowKeys((long key) -> {
                                    // Invalidate the cached index table for the modified location.
                                    final TableLocation location = locationColumnSource.get(key);

                                    final LocationState locationState = locations.get(location);
                                    locationState.cachedIndexTable = null;
                                });
                            }
                        }
                    };
            locationTable.addUpdateListener(validatorTableListener);
        }

        // We will defer the actual index creation until it is needed.
    }

    private final LinkedHashMap<TableLocation, LocationState> locations = new LinkedHashMap<>();

    @Override
    public String[] keyColumnNames() {
        return new String[0];
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
                    "usePrev==true is not supported for refreshing storage-backed data index tables");
        }

        if (indexTable == null) {
            indexTable = QueryPerformanceRecorder
                    .withNugget("Build Storage Backed Data Index [" + String.join(", ", keyColumnNames) + "]", () -> {

                        final Table[] locationIndexes = new Table[locations.size()];
                        int tCount = 0;
                        for (final LocationState ls : locations.values()) {
                            final Table locationIndex = ls.getCachedIndexTable();
                            // If any location is missing a data index, we must bail out because we can't guarantee a
                            // consistent
                            // index.
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
            cachedPositionLookup = buildPositionLookup(indexTable, keyColumnNames());
        } else {
            // Build or use the hashmap
            if (cachedPositionMap == null) {
                cachedPositionMap = buildPositionMap(indexTable, keyColumnNames());
            }
            cachedPositionLookup = cachedPositionMap::get;
        }
        return cachedPositionLookup;
    }

    @Override
    public boolean isRefreshing() {
        return false;
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
    // endregion
}
