package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.dataindex.BaseDataIndex;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.*;
import java.util.stream.IntStream;

/**
 * DataIndex that accumulates the individual per-{@link TableLocation} data indexes of a {@link SourceTable} backed by a
 * {@link RegionedColumnSourceManager}.
 * 
 * @implNote This implementation is responsible for ensuring that the provided table accounts for the relative positions
 *           of individual table locations in the provided table of indices. Work to coalesce the index table is
 *           deferred until the first call to {@link #table()}.
 */
@InternalUseOnly
class MergedDataIndex extends BaseDataIndex {

    private static final String LOCATION_DATA_INDEX_TABLE_COLUMN_NAME = "__DataIndexTable";
    private static final String ROW_KEY_OFFSET_COLUMN_NAME = "__RowKeyOffset";

    private final String[] keyColumnNames;
    private final ColumnSourceManager columnSourceManager;
    private final Map<ColumnSource<?>, String> keyColumnMap;

    /** The partitioned table containing the location index tables. */
    private final QueryTable partitions;
    private final ObjectArraySource<Table> locationDataIndexTables;

    private final ModifiedColumnSet upstreamLocationModified;

    private final ArrayList<LocationState> locationStates;

    /** The table containing the index. Consists of sorted key column(s) and an associated RowSet column. */
    private QueryTable indexTable;

    private AggregationRowLookup lookupFunction;

    /** Whether this index is known to be corrupt. */
    private boolean isCorrupt = false;

    MergedDataIndex(
            @NotNull final String[] keyColumnNames,
            @NotNull final ColumnSource<?>[] keySources,
            @NotNull final ColumnSourceManager columnSourceManager) {

        Require.eq(keyColumnNames.length, "keyColumnNames.length", keySources.length, "keySources.length");

        this.keyColumnNames = keyColumnNames;
        this.columnSourceManager = columnSourceManager;

        // Create an in-order reverse lookup map for the key column names
        keyColumnMap = Collections.unmodifiableMap(IntStream.range(0, keySources.length).sequential()
                .collect(LinkedHashMap::new, (m, i) -> m.put(keySources[i], keyColumnNames[i]), Assert::neverInvoked));


        // Create the column source for the locations. The keys for this column source are the row keys of the
        // location table.
        locationStates = new ArrayList<>(); // TODO-RWC: WHAT IS THIS FOR?

        // Create the underlying partitions table to hold the materialized location data indexes.
        final QueryTable locationTable = (QueryTable) columnSourceManager.locationTable().coalesce();
        final Map<String, ColumnSource<?>> partitionsColumnSourceMap = new LinkedHashMap<>();
        locationDataIndexTables = new ObjectArraySource<>(Table.class);
        partitionsColumnSourceMap.put(LOCATION_DATA_INDEX_TABLE_COLUMN_NAME, locationDataIndexTables);
        partitions = new QueryTable(RowSetFactory.empty().toTracking(), partitionsColumnSourceMap);

        if (locationTable.isRefreshing()) {
            upstreamLocationModified = locationTable.newModifiedColumnSet(columnSourceManager.locationColumnName());
            final BaseTable.ListenerImpl locationUpdateListener =
                    new BaseTable.ListenerImpl(String.format(
                            "Merged Data Index - %s", Arrays.toString(keyColumnNames)), locationTable, partitions) {
                        @Override
                        public void onUpdate(@NotNull final TableUpdate upstream) {
                            processUpdate(getParent(), upstream, false);
                        }
                    };
            locationTable.addUpdateListener(locationUpdateListener);
            manage(partitions); // TODO-RWC: MAKE SURE WE MANAGE THE EVENTUAL RESULT? OR IS THAT NOT OUR PROBLEM?
        } else {
            upstreamLocationModified = null;
        }

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

        // Defer the actual index table creation until it is needed
    }

    private synchronized void processUpdate(
            @NotNull final Table locationTable,
            @NotNull final TableUpdate upstream,
            final boolean initializing) {
        if (upstream.empty()) {
            return;
        }
        if (upstream.removed().isNonempty()) {
            throw new UnsupportedOperationException("Removed locations are not currently supported");
        }
        if (upstream.shifted().nonempty()) {
            throw new UnsupportedOperationException("Shifted locations are not currently supported");
        }
        if (upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(upstreamLocationModified)) {
            throw new UnsupportedOperationException("Modified locations are not currently supported");
        }
        Assert.assertion(initializing || isRefreshing(), "initializing || isRefreshing()");

        if (upstream.added().isEmpty()) {
            return;
        }

        final long previousSize = partitions.size();
        final long newSize = locationTable.size();
        Assert.eq(newSize - previousSize, "newSize - previousSize", upstream.added().size(), "upstream.added().size()");

        locationDataIndexTables.ensureCapacity(newSize);

        final ColumnSource<TableLocation> locationColumnSource =
                locationTable.getColumnSource(columnSourceManager.locationColumnName(), TableLocation.class);

        // Resize the column sources to accommodate the new rows.

        // Ensure the location array list is large enough to hold the new rows.
        final int newLocationsSize = Math.toIntExact(locationStates.size() + upstream.added().size());
        locationStates.ensureCapacity(newLocationsSize);

        upstream.added().forAllRowKeys((long key) -> {
            final TableLocation location = locationTableColumnSource.get(key);
            final LocationState locationState = new LocationState(
                    location,
                    RegionedColumnSource.getFirstRowKey(Math.toIntExact(key)),
                    keyColumnNames);
            // Added location keys should always be dense.
            Assert.eq(key, "key", locationStates.size(), "locationStates.size()");
            locationStates.add((int) key, locationState);

            if (generateUpdates) {
                // Since the index table is already materialized, we need to load the location data index table
                // and add it to the partitioned table.
                try {
                    final Table locationIndexTable = locationState.getCachedIndexTable();
                    locationDataIndexTables.set(key, locationIndexTable);
                    addedBuilder.appendKey(key);
                } catch (Exception e) {
                    // Mark this index as corrupted.
                    isCorrupt = true;
                    throw new IllegalStateException("Failed to update data index.", e);
                }
            }
        });

        if (previousSize == newSize) {
            return;
        }

        partitions.getRowSet().writableCast().insertRange(previousSize, newSize - 1);

        if (!initializing) {
            return;
        }

        final TableUpdate downstream = new TableUpdateImpl(
                RowSetFactory.fromRange(previousSize, newSize - 1),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);
        partitions.notifyListeners(downstream);
    }

    @Override
    @NotNull
    public String[] keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnMap() {
        return keyColumnMap;
    }

    @Override
    @NotNull
    public String rowSetColumnName() {
        return ROW_SET_COLUMN_NAME;
    }

    @Override
    @NotNull
    public Table table() {
        if (indexTable != null) {
            return indexTable;
        }

        synchronized (this) {
            // Test again under the lock.
            if (indexTable != null) {
                return indexTable;
            }

            indexTable = QueryPerformanceRecorder.withNugget(
                    "Build Storage Backed Data Index [" + String.join(", ", keyColumnNames) + "]",
                    () -> {
                        // Populate the partitions table with location index tables.
                        final long size = locationStates.size();
                        locationDataIndexTables.ensureCapacity(size);
                        long partitionRowKey = 0;
                        try {
                            // TODO: load all the data indexes in parallel. Can we use the JobScheduler and
                            // block on a future? Or would that lead to a deadlock if this is running on a UGP thread?
                            // How can we tell if this is UGP or initialization pool?
                            for (final LocationState ls : locationStates) {
                                final Table locationIndexTable = ls.getCachedIndexTable();
                                locationDataIndexTables.set(partitionRowKey++, locationIndexTable);
                            }
                        } catch (Exception e) {
                            // Mark this index as corrupted.
                            isCorrupt = true;
                            throw new IllegalStateException("Failed to load data index.", e);
                        }
                        partitions.getRowSet().writableCast().insertRange(0, size - 1);

                        // Merge all the location index tables into a single table then partition it by the keys.
                        final Table merged = PartitionedTableFactory.of(partitions).merge();
                        final PartitionedTable pt = merged.partitionBy(keyColumnNames);

                        lookupFunction = AggregationProcessor.getRowLookup(pt.table());
                        Assert.neqNull(lookupFunction,
                                "AggregationRowLookup lookupFunction should never be null");

                        // Transform the partitioned table to create a new table with a single row set column.
                        final PartitionedTable transformed = pt.transform(t -> {
                            // Create a new table with only one row, containing the key columns and the merged RowSet.
                            Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
                            for (String keyColumnName : keyColumnNames) {
                                columnSourceMap.put(keyColumnName, t.getColumnSource(keyColumnName));
                            }

                            // Build a new row set from the individual row sets (with their offset keys).
                            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                            try (final CloseableIterator<RowSet> rsIt = t.columnIterator(ROW_SET_COLUMN_NAME);
                                    final CloseableIterator<Long> keyIt =
                                            t.columnIterator(ROW_KEY_OFFSET_COLUMN_NAME)) {
                                while (rsIt.hasNext()) {
                                    final RowSet rowSet = rsIt.next();
                                    final long offsetKey = keyIt.next();
                                    builder.appendRowSequenceWithOffset(rowSet, offsetKey);
                                }
                            }
                            final RowSet outputRowSet = builder.build();

                            // Create a SingleValueColumnSource for the row set and add it to the column
                            // source map.
                            SingleValueColumnSource<RowSet> rowSetColumnSource =
                                    SingleValueColumnSource.getSingleValueColumnSource(RowSet.class);
                            rowSetColumnSource.set(outputRowSet);
                            columnSourceMap.put(ROW_SET_COLUMN_NAME, rowSetColumnSource);

                            // The result table row set is a single key. We'll use the first key of the input
                            // table to get the correct key values from the key column sources.
                            final WritableRowSet resultRowSet =
                                    RowSetFactory.fromKeys(t.getRowSet().firstRowKey());

                            return new QueryTable(resultRowSet.toTracking(), columnSourceMap);
                        });

                        // Flatten the result table to cache all the redirections we just created.
                        final Table mergedOutput = transformed.merge();

                        final QueryTable result =
                                indexTableWrapper((QueryTable) mergedOutput.select(), ROW_SET_COLUMN_NAME);
                        result.setRefreshing(columnSourceManager.locationTable().isRefreshing());

                        return result;
                    });
        }

        return indexTable;
    }

    @Override
    @NotNull
    public RowSetLookup rowSetLookup() {
        final ColumnSource<RowSet> rowSetColumnSource = rowSetColumn();
        return (Object key, boolean usePrev) -> {
            // Pass the object to the position lookup, then return the row set at that position.
            final int position = lookupFunction.get(key);
            if (position == AggregationRowLookup.DEFAULT_UNKNOWN_ROW) {
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
    @NotNull
    public PositionLookup positionLookup() {
        return (Object key, boolean usePrev) -> {
            // Pass the object to the aggregation lookup, then return the resulting position. This index will be
            // correct in prev or current space because of the aggregation's hash-based lookup.
            return lookupFunction.get(key);
        };
    }

    @Override
    public boolean isRefreshing() {
        return columnSourceManager.locationTable().isRefreshing();
    }

    private static class LocationState {

        private final TableLocation location;
        private final long offsetKey;
        private final String[] keyColumns;

        private volatile SoftReference<Table> cachedIndexTableReference;

        private LocationState(final TableLocation location,
                final long offsetKey,
                @NotNull final String... keyColumns) {
            this.location = location;
            this.offsetKey = offsetKey;
            this.keyColumns = keyColumns;
        }

        @Nullable
        private Table getCachedIndexTable() {
            SoftReference<Table> localCachedIndexTableReference;
            Table localCachedIndexTable;

            // Already cached?
            if ((localCachedIndexTableReference = cachedIndexTableReference) != null
                    && (localCachedIndexTable = localCachedIndexTableReference.get()) != null) {
                return localCachedIndexTable;
            }

            synchronized (this) {
                // Now already cached?
                if ((localCachedIndexTableReference = cachedIndexTableReference) != null
                        && (localCachedIndexTable = localCachedIndexTableReference.get()) != null) {
                    return localCachedIndexTable;
                }

                // I guess not... load it.
                Table indexTable = location.getDataIndex(keyColumns);
                if (indexTable != null) {
                    final Map<String, ColumnSource<?>> columnSourceMap =
                            new LinkedHashMap<>(indexTable.getColumnSourceMap());

                    // Record the first key as a column of this table using a SingleValueColumnSource.
                    final SingleValueColumnSource<?> offsetKeySource =
                            SingleValueColumnSource.getSingleValueColumnSource(long.class);
                    offsetKeySource.set(offsetKey);
                    columnSourceMap.put(ROW_KEY_OFFSET_COLUMN_NAME, offsetKeySource);

                    indexTable = new QueryTable(indexTable.getRowSet(), columnSourceMap);
                    cachedIndexTableReference = new SoftReference<>(indexTable);
                }
                return indexTable;
            }
        }
    }
    // endregion

    @Override
    public boolean validate() {
        if (isCorrupt) {
            return false;
        }
        // Examine the locations for this index and validate that every location has a data index.
        for (final LocationState ls : locationStates) {
            // The Location test is fast but incomplete.
            if (!ls.location.hasDataIndex(keyColumnNames)) {
                return false;
            }
        }
        return true;

    }

    private final class UncoalescedLocationIndexTable extends UncoalescedTable<UncoalescedLocationIndexTable> {

        private final TableLocation location;
        private final long locationFirstRowKey;

        public UncoalescedLocationIndexTable(
                @NotNull final TableDefinition definition,
                @NotNull final TableLocation location,
                final long locationFirstRowKey) {
            super(definition, String.format("UncoalescedLocationIndexTable-%s", location));
            this.location = location;
            this.locationFirstRowKey = locationFirstRowKey;
        }

        @Override
        protected UncoalescedLocationIndexTable copy() {
            return new UncoalescedLocationIndexTable(definition, location);
        }

        @Override
        protected Table doCoalesce() {
            final Table indexTable = location.getDataIndex(keyColumnNames);
            if (indexTable == null) {
                return null;
            }
            final Map<String, ColumnSource<?>> columnSourceMap =
                    new LinkedHashMap<>(indexTable.getColumnSourceMap());

            // Record the first key as a column of this table using a SingleValueColumnSource
            final SingleValueColumnSource<?> rowKeyOffsetSource =
                    SingleValueColumnSource.getSingleValueColumnSource(long.class);
            rowKeyOffsetSource.set(locationFirstRowKey);
            columnSourceMap.put(ROW_KEY_OFFSET_COLUMN_NAME, rowKeyOffsetSource);

            return new QueryTable(indexTable.getRowSet(), columnSourceMap);
        }
    }
}
