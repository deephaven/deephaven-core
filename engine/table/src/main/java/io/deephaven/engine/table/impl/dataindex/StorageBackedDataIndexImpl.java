package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.*;

import static io.deephaven.engine.table.impl.partitioned.PartitionedTableCreatorImpl.CONSTITUENT;

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

    @NotNull
    final String[] keyColumnNames;

    /** The table containing the index. Consists of sorted key column(s) and an associated RowSet column. */
    private QueryTable indexTable;
    /** The partitioned table containing the location index tables. */
    private final QueryTable partitions;
    private final ObjectArraySource<Table> partitionTableConstituentSource;
    private final ModifiedColumnSet partitionsConstituentModifiedColumnSet;

    private final ArrayList<LocationState> locationStates;

    private AggregationRowLookup lookupFunction;

    /** Whether this index is known to be corrupt. */
    private boolean isCorrupt = false;

    public StorageBackedDataIndexImpl(
            @NotNull final ColumnSource<?>[] keySources,
            @NotNull final ColumnSourceManager columnSourceManager,
            @NotNull final String[] keyColumnNames) {

        this.columnSourceManager = columnSourceManager;
        this.keyColumnNames = keyColumnNames;

        // Create an in-order reverse lookup map for the key columnn names.
        keyColumnMap = new WeakHashMap<>(keySources.length);
        for (int ii = 0; ii < keySources.length; ii++) {
            keyColumnMap.put(keySources[ii], keyColumnNames[ii]);
        }

        // Store the column source manager for later use.
        final Table locationTable = columnSourceManager.locationTable();

        // Create the column source for the locations. The keys for this column source are the row keys of the
        // location table.
        locationStates = new ArrayList<>();

        // Create the underlying partitions table to hold the materialized location data indexes.
        final Map<String, ColumnSource<?>> partitionsColumnSourceMap = new LinkedHashMap<>();
        partitionTableConstituentSource = new ObjectArraySource<>(Table.class);
        partitionsColumnSourceMap.put(CONSTITUENT.name(), partitionTableConstituentSource);
        partitions = new QueryTable(RowSetFactory.empty().toTracking(), partitionsColumnSourceMap);
        partitionsConstituentModifiedColumnSet = partitions.newModifiedColumnSet(CONSTITUENT.name());

        if (locationTable.isRefreshing()) {
            partitions.setRefreshing(true);
            final TableUpdateListener locationUpdateListener =
                    new InstrumentedTableUpdateListenerAdapter(locationTable, false) {
                        @Override
                        public void onUpdate(TableUpdate upstream) {
                            processUpdate(upstream, false);
                        }
                    };
            locationTable.addUpdateListener(locationUpdateListener);
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

        // We will track updates only when we have already materialized the index table.
        final boolean trackUpdates = isRefreshing() && indexTable != null;

        final RowSetBuilderSequential addedBuilder = trackUpdates ? null : RowSetFactory.builderSequential();
        final RowSetBuilderRandom modifiedBuilder = trackUpdates ? null : RowSetFactory.builderRandom();

        // Get the location column from the RegionedColumnSourceManager.
        final ColumnSource<TableLocation> locationTableColumnSource =
                columnSourceManager.locationTable().getColumnSource(columnSourceManager.locationColumnName());

        if (update.added().isNonempty()) {
            // Resize the column sources to accommodate the new rows.
            if (trackUpdates) {
                final long newSize = partitions.size() + update.added().lastRowKey() + 1;
                partitionTableConstituentSource.ensureCapacity(newSize);
            }

            update.added().forAllRowKeys((long key) -> {
                final TableLocation location = locationTableColumnSource.get(key);
                final LocationState locationState = new LocationState(
                        location,
                        RegionedColumnSource.getFirstRowKey(Math.toIntExact(key)),
                        keyColumnNames);
                // Added location keys should always be dense and consecutive.
                Assert.eq(key, "key", locationStates.size() + 1, "locationStates.size() + 1");
                locationStates.set((int)key, locationState);

                if (trackUpdates) {
                    // Since the index table is already materialized, we need to load the location data index table
                    // and add it to the partitioned table.
                    try {
                        final Table locationIndexTable = locationState.getCachedIndexTable();
                        partitionTableConstituentSource.set(key, locationIndexTable);
                        addedBuilder.appendKey(key);
                    } catch (Exception e) {
                        // Mark this index as corrupted.
                        isCorrupt = true;
                        throw new IllegalStateException("Failed to update data index.", e);
                    }
                }
            });
        }

        update.modified().forAllRowKeys((long key) -> {
            final LocationState locationState = locationStates.get((int)key);
            // Reset the cached index table for the modified location states.
            locationState.cachedIndexTable = null;

            if (trackUpdates) {
                // Since the index table is already materialized, we need to load the new location data index table
                // and replace it in the partitioned table.
                try {
                    final Table locationIndexTable = locationState.getCachedIndexTable();
                    partitionTableConstituentSource.set(key, locationIndexTable);
                    modifiedBuilder.addKey(key);
                } catch (Exception e) {
                    // Mark this index as corrupted.
                    isCorrupt = true;
                    throw new IllegalStateException("Failed to update data index.", e);
                }
            }
        });

        if (trackUpdates) {
            // Send an updates to the partitioned table listeners.
            final RowSet added = addedBuilder.build();
            final RowSet modified = modifiedBuilder.build();

            final TableUpdate downstream = new TableUpdateImpl(
                    added,
                    modified,
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    modified.isNonempty() ? partitionsConstituentModifiedColumnSet : ModifiedColumnSet.EMPTY);

            if (!downstream.empty()) {
                partitions.notifyListeners(downstream);
            }
        }
    }

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
    public Table table() {
        if (indexTable != null) {
            return indexTable;
        }

        synchronized (this) {
            // Test again under the lock.
            if (indexTable != null) {
                return indexTable;
            }

            indexTable = QueryPerformanceRecorder.withNugget("Build Storage Backed Data Index [" + String.join(", ", keyColumnNames) + "]",
                () -> {
                    // Populate the partitions table with location index tables.
                    final long size = locationStates.size();
                    partitionTableConstituentSource.ensureCapacity(size);
                    long partitionRowKey = 0;
                    try {
                        // TODO: load all the data indexes in parallel. Can we use the JobScheduler and
                        //  block on a future? Or would that lead to a deadlock if this is running on a UGP thread?
                        //  How can we tell if this is UGP or initialization pool?
                        for (final LocationState ls : locationStates) {
                            final Table locationIndexTable = ls.getCachedIndexTable();
                            partitionTableConstituentSource.set(partitionRowKey++, locationIndexTable);
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

                    // Transform the partitioned table to create a new table with a single row set column.
                    final PartitionedTable transformed = pt.transform(t -> {
                        // Create a new table with only one row, containing the key columns and the merged RowSet.
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

                        // Create a SingleValueColumnSource for the row set and add it to the column
                        // source map.
                        SingleValueColumnSource<RowSet> rowSetColumnSource =
                                SingleValueColumnSource.getSingleValueColumnSource(RowSet.class);
                        rowSetColumnSource.set(outputRowSet);
                        columnSourceMap.put(INDEX_COL_NAME, rowSetColumnSource);

                        // The result table row set is a single key. We'll use the first key of the input
                        // table to get the correct key values from the key column sources.
                        final WritableRowSet resultRowSet =
                                RowSetFactory.fromKeys(t.getRowSet().firstRowKey());

                        return new QueryTable(resultRowSet.toTracking(), columnSourceMap);
                    });

                    // Flatten the result table to cache all the redirections we just created.
                    final Table mergedOutput = transformed.merge();

                    // TODO: need to be able to get the lookup function from the merged PartitionedTable
                    lookupFunction = AggregationProcessor.getRowLookup(mergedOutput);
                    Assert.neqNull(lookupFunction,
                            "AggregationRowLookup lookupFunction should never be null");

                    final QueryTable result = wrappedRowSetTable((QueryTable) mergedOutput.select(), INDEX_COL_NAME);
                    result.setRefreshing(columnSourceManager.locationTable().isRefreshing());

                    return result;
                });
        }

        return indexTable;
    }

    @Override
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
    public @NotNull PositionLookup positionLookup() {
        return (Object key, boolean usePrev) -> {
            // Pass the object to the aggregation lookup, then return the resulting position. This index will be
            // correct in prev or current space because of the aggregation's hash-based lookup.
            return lookupFunction.get(key);
        };
    }

    @Override
    public boolean isRefreshing() {
        return sourceTable.isRefreshing();
    }

    @Override
    public Table baseIndexTable() {
        return table();
    }

    private static class LocationState {
        private final TableLocation location;-
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
}
