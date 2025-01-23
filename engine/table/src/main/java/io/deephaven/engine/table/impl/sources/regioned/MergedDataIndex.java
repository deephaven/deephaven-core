//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.ThreadSafeCounter;
import io.deephaven.base.stats.Value;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ForkJoinPoolOperationInitializer;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.dataindex.AbstractDataIndex;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.select.MultiSourceFunctionalColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * DataIndex that accumulates the individual per-{@link TableLocation} data indexes of a {@link Table} backed by a
 * {@link RegionedColumnSourceManager}.
 *
 * @implNote This implementation is responsible for ensuring that the provided table accounts for the relative positions
 *           of individual table locations in the provided table of indices. Work to coalesce the index table is
 *           deferred until the first call to {@link #table()}. Refreshing inputs/indexes are not supported at this time
 *           due to concurrency limitations (w.r.t. the UpdateGraph) of the underlying table operations used to compute
 *           the merged index table, as well as a lack of use cases beyond "new static partitions are added to a live
 *           source table".
 */
@InternalUseOnly
class MergedDataIndex extends AbstractDataIndex implements DataIndexer.RetainableDataIndex {
    /**
     * The duration in nanos to build a DataIndex table.
     */
    private static final Value BUILD_INDEX_TABLE_MILLIS = Stats
            .makeItem("MergedDataIndex", "buildTableMillis", ThreadSafeCounter.FACTORY,
                    "Duration in millis of building an index")
            .getValue();

    /**
     * When merging row sets from multiple component DataIndex structures, reading each individual component can be
     * performed in parallel to improve the wall-clock I/O time observed. For cases where the number of individual
     * groups to merge is small and I/O bound this can be very beneficial. Setting up the parallelism, however, can add
     * additional overhead. Setting the Configuration property "MergedDataIndex.useParallelLazyFetch" to false disables
     * this behavior and each key's merged RowSet is computed serially.
     */
    public static boolean USE_PARALLEL_LAZY_FETCH = Configuration.getInstance()
            .getBooleanWithDefault("MergedDataIndex.useParallelLazyFetch", true);

    private static final String LOCATION_DATA_INDEX_TABLE_COLUMN_NAME = "__DataIndexTable";

    private final List<String> keyColumnNames;
    private final RegionedColumnSourceManager columnSourceManager;

    private final Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn;

    /**
     * The lookup function for the index table. Note that this is always set before {@link #indexTable}.
     */
    private AggregationRowLookup lookupFunction;

    /**
     * The index table. Note that we use this as a barrier to ensure {@link #lookupFunction} is visible.
     */
    private volatile Table indexTable;

    /**
     * A lazy version of the table. This value is never set if indexTable is set.
     */
    private volatile Table lazyTable;

    /**
     * If we have a lazy version of the table, we hold onto the partitioned table, which permits us to select the
     * underlying rowsets without needing to reselect the key columns.
     */
    private volatile PartitionedTable lazyPartitionedTable;

    /**
     * Whether this index is known to be corrupt.
     */
    private volatile boolean isCorrupt;

    /**
     * Whether this index is valid. {@code null} means we don't know, yet.
     */
    private volatile Boolean isValid;

    MergedDataIndex(
            @NotNull final String[] keyColumnNames,
            @NotNull final ColumnSource<?>[] keySources,
            @NotNull final RegionedColumnSourceManager columnSourceManager) {
        Require.eq(keyColumnNames.length, "keyColumnNames.length", keySources.length, "keySources.length");
        Require.elementsNeqNull(keyColumnNames, "keyColumnNames");
        Require.elementsNeqNull(keySources, "keySources");

        this.keyColumnNames = List.of(keyColumnNames);
        this.columnSourceManager = columnSourceManager;

        // Create an in-order reverse lookup map for the key column names
        keyColumnNamesByIndexedColumn = Collections.unmodifiableMap(IntStream.range(0, keySources.length).sequential()
                .collect(LinkedHashMap::new, (m, i) -> m.put(keySources[i], keyColumnNames[i]), Assert::neverInvoked));
        if (keyColumnNamesByIndexedColumn.size() != keySources.length) {
            throw new IllegalArgumentException(String.format("Duplicate key sources found in %s for %s",
                    Arrays.toString(keySources), Arrays.toString(keyColumnNames)));
        }

        if (columnSourceManager.locationTable().isRefreshing()) {
            throw new UnsupportedOperationException("Refreshing location tables are not currently supported");
        }

        // Defer the actual index table creation until it is needed
    }

    @Override
    @NotNull
    public List<String> keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
        return keyColumnNamesByIndexedColumn;
    }

    @Override
    @NotNull
    public Table table(final DataIndexOptions options) {
        Table localIndexTable;
        if ((localIndexTable = indexTable) != null) {
            return localIndexTable;
        }
        final boolean lazyRowSetMerge = options.operationUsesPartialTable();
        if (lazyRowSetMerge) {
            if ((localIndexTable = lazyTable) != null) {
                return localIndexTable;
            }
        }
        synchronized (this) {
            if ((localIndexTable = indexTable) != null) {
                return localIndexTable;
            } else if (lazyRowSetMerge) {
                if ((localIndexTable = lazyTable) != null) {
                    return localIndexTable;
                }
            }
            try {
                return QueryPerformanceRecorder.withNugget(
                        String.format("Merge Data Indexes [%s]", String.join(", ", keyColumnNames)),
                        ForkJoinPoolOperationInitializer.ensureParallelizable(() -> buildTable(lazyRowSetMerge)));
            } catch (Throwable t) {
                isCorrupt = true;
                throw t;
            }
        }
    }

    /**
     * The RowSetCacher is a bit of a hack that allows us to avoid reading actual RowSets from disk until they are
     * actually required for a query operation. We are breaking engine rules in that we reference the source
     * ColumnSource directly and do not have correct dependencies encoded in a select. MergedDataIndexes are only
     * permitted for a static table, so we can get away with this.
     *
     * <p>
     * Once a RowSet has been written, we write down the result into an AtomicReferenceArray so that it need not be read
     * repeatedly. If two threads attempt to concurrently fetch the same RowSet, then we use a placeholder object in the
     * array to avoid duplicated work.
     * </p>
     */
    private static class RowSetCacher {
        final ColumnSource<ObjectVector<RowSet>> source;
        final AtomicReferenceArray<Object> results;

        private RowSetCacher(final ColumnSource<ObjectVector<RowSet>> source, final int capacity) {
            this.source = source;
            this.results = new AtomicReferenceArray<>(capacity);
        }

        RowSet get(final long rowKey) {
            if (rowKey < 0 || rowKey >= results.length()) {
                return null;
            }

            final int iRowKey = (int) rowKey;
            do {
                final Object localResult = results.get(iRowKey);
                if (localResult instanceof RowSet) {
                    return (RowSet) localResult;
                }
                if (localResult instanceof Exception) {
                    throw new UncheckedDeephavenException("Exception found for cached RowSet",
                            (Exception) localResult);
                }

                if (localResult != null) {
                    // noinspection EmptySynchronizedStatement
                    synchronized (localResult) {
                        // don't care to do anything, we are just waiting for the barrier to be done
                    }
                    continue;
                }

                // we need to create our own placeholder, and synchronize on it first
                final Object placeholder = new Object();
                // noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (placeholder) {
                    if (!results.compareAndSet(iRowKey, null, placeholder)) {
                        // we must try again, someone else has claimed the placeholder
                        continue;
                    }
                    // We won the race, so it is our responsibility to get the right answer

                    final ObjectVector<RowSet> inputRowSets = source.get(rowKey);
                    Assert.neqNull(inputRowSets, "inputRowSets");
                    final RowSet computedResult;
                    try {
                        if (USE_PARALLEL_LAZY_FETCH) {
                            // noinspection DataFlowIssue
                            computedResult = mergeRowSetsParallel(rowKey, inputRowSets);
                        } else {
                            // noinspection DataFlowIssue
                            computedResult = mergeRowSetsSerial(rowKey, inputRowSets);
                        }
                    } catch (Exception e) {
                        results.set(iRowKey, e);
                        throw e;
                    }
                    results.set(iRowKey, computedResult);
                    return computedResult;
                }
            } while (true);
        }
    }

    private Table buildTable(final boolean lazyRowSetMerge) {
        if (lazyTable != null) {
            if (lazyRowSetMerge) {
                return lazyTable;
            }
        }

        final long t0 = System.nanoTime();
        try {
            final PartitionedTable partitionedTable;
            if (lazyPartitionedTable != null) {
                // We are synchronized, and can begin processing from the PartitionedTable rather than starting from
                // scratch. The first step is to force our rowsets into memory, in parallel.
                partitionedTable = lazyPartitionedTable.transform(t -> ForkJoinPoolOperationInitializer
                        .ensureParallelizable(() -> t.update(ROW_SET_COLUMN_NAME)).get());
            } else {
                partitionedTable = buildPartitionedTable(lazyRowSetMerge);
            }

            // Merge all the location index tables into a single table
            final Table mergedDataIndexes = partitionedTable.merge();
            // Group the merged data indexes by the keys
            final Table groupedByKeyColumns = mergedDataIndexes.groupBy(keyColumnNames.toArray(String[]::new));
            lookupFunction = AggregationProcessor.getRowLookup(groupedByKeyColumns);

            final Table combined;
            if (lazyRowSetMerge) {
                final ColumnSource<ObjectVector<RowSet>> vectorColumnSource =
                        groupedByKeyColumns.getColumnSource(ROW_SET_COLUMN_NAME);

                // we are using a regular array here; so we must ensure that we are flat; or we'll have a bad time
                Assert.assertion(groupedByKeyColumns.isFlat(), "groupedByKeyColumns.isFlat()");
                final RowSetCacher rowsetCacher = new RowSetCacher(vectorColumnSource, groupedByKeyColumns.intSize());
                combined = groupedByKeyColumns
                        .view(List.of(SelectColumn.ofStateless(new MultiSourceFunctionalColumn<>(List.of(),
                                ROW_SET_COLUMN_NAME, RowSet.class, (k, v) -> rowsetCacher.get(k)))));

                lazyPartitionedTable = partitionedTable;
                lazyTable = combined;
            } else {
                // Combine the row sets from each group into a single row set
                final List<SelectColumn> mergeFunction = List.of(SelectColumn.ofStateless(new FunctionalColumn<>(
                        ROW_SET_COLUMN_NAME, ObjectVector.class,
                        ROW_SET_COLUMN_NAME, RowSet.class,
                        MergedDataIndex::mergeRowSetsSerial)));

                combined = groupedByKeyColumns.update(mergeFunction);

                indexTable = combined;
                // if we were built off a lazy table, null it out now that we've setup the indexTable
                lazyPartitionedTable = null;
                lazyTable = null;
            }
            Assert.assertion(combined.isFlat(), "combined.isFlat()");
            Assert.eq(groupedByKeyColumns.size(), "groupedByKeyColumns.size()", combined.size(), "combined.size()");

            return combined;
        } finally {
            final long t1 = System.nanoTime();
            BUILD_INDEX_TABLE_MILLIS.sample((t1 - t0) / 1_000_000);
        }
    }

    /**
     * Perform a parallelizable update to produce coalesced location index tables with their row sets shifted by the
     * appropriate region offset. The keys are always forced into memory so that the groupBy operation need not read
     * from disk serially.
     * <p>
     * If lazyRowSetMerge is true, the rowsets are read into memory as part of the mergeRowSets call and are not forced
     * into memory here. If lazyRowSetMerge is false, then the RowSets are also forced into memory.
     * </p>
     */
    private PartitionedTable buildPartitionedTable(final boolean lazyRowSetMerge) {
        final String[] keyColumnNamesArray = keyColumnNames.toArray(String[]::new);
        final Table locationTable = columnSourceManager.locationTable().coalesce();
        final Table locationDataIndexes = locationTable
                .update(List.of(SelectColumn.ofStateless(new FunctionalColumn<>(
                        columnSourceManager.locationColumnName(), TableLocation.class,
                        LOCATION_DATA_INDEX_TABLE_COLUMN_NAME, Table.class,
                        (final long locationRowKey, final TableLocation location) -> loadIndexTableAndShiftRowSets(
                                locationRowKey, location, keyColumnNamesArray, !lazyRowSetMerge)))))
                .dropColumns(columnSourceManager.locationColumnName());

        return PartitionedTableFactory.of(locationDataIndexes);
    }

    private static Table loadIndexTableAndShiftRowSets(
            final long locationRowKey,
            @NotNull final TableLocation location,
            @NotNull final String[] keyColumnNames,
            final boolean selectRowSets) {
        final BasicDataIndex dataIndex = location.getDataIndex(keyColumnNames);
        if (dataIndex == null) {
            throw new UncheckedDeephavenException(String.format("Failed to load data index [%s] for location %s",
                    String.join(", ", keyColumnNames), location));
        }
        final Table indexTable = dataIndex.table();
        final long shiftAmount = RegionedColumnSource.getFirstRowKey(Math.toIntExact(locationRowKey));
        final Table coalesced = indexTable.coalesce();

        final Selectable shiftFunction;
        if (shiftAmount == 0) {
            // A source column would be more convenient, but we are going to close the RowSet after we are done merging
            // it and should not allow that close call to pass through to the original table.
            shiftFunction = new FunctionalColumn<>(
                    dataIndex.rowSetColumnName(), RowSet.class,
                    ROW_SET_COLUMN_NAME, RowSet.class,
                    RowSet::copy);
        } else {
            shiftFunction = new FunctionalColumn<>(
                    dataIndex.rowSetColumnName(), RowSet.class,
                    ROW_SET_COLUMN_NAME, RowSet.class,
                    (final RowSet rowSet) -> rowSet.shift(shiftAmount));
        }
        if (selectRowSets) {
            // pull the key columns into memory while we are parallel; and read all the rowsets (by virtue of the shift)
            final List<Selectable> columns = new ArrayList<>(keyColumnNames.length + 1);
            Arrays.stream(keyColumnNames).map(ColumnName::of).forEach(columns::add);
            columns.add(SelectColumn.ofStateless(shiftFunction));
            return ForkJoinPoolOperationInitializer.ensureParallelizable(() -> coalesced.update(columns)).get();
        } else {
            // pull the key columns into memory while we are parallel; but do not read all the RowSets
            final Table withInMemoryKeyColumns =
                    ForkJoinPoolOperationInitializer.ensureParallelizable(() -> coalesced.update(keyColumnNames)).get();
            return withInMemoryKeyColumns.updateView(List.of(SelectColumn.ofStateless(shiftFunction)));
        }
    }

    /**
     * The returned RowSet is owned by the caller. The input RowSets are closed.
     */
    private static RowSet mergeRowSetsSerial(
            @SuppressWarnings("unused") final long unusedRowKey,
            @NotNull final ObjectVector<RowSet> keyRowSets) {
        final long numRowSets = keyRowSets.size();

        if (numRowSets == 1) {
            // we steal the reference, the input is never used again
            return keyRowSets.get(0);
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        try (final CloseableIterator<RowSet> rowSets = keyRowSets.iterator()) {
            rowSets.forEachRemaining(rs -> {
                builder.appendRowSequence(rs);
                rs.close();
            });
        }

        return builder.build();
    }

    /**
     * The returned RowSet is owned by the caller. The input RowSets are closed.
     */
    private static RowSet mergeRowSetsParallel(
            @SuppressWarnings("unused") final long unusedRowKey,
            @NotNull final ObjectVector<RowSet> keyRowSets) {
        final long numRowSets = keyRowSets.size();

        if (numRowSets == 1) {
            // we steal the reference, the input is never used again
            return keyRowSets.get(0);
        }
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

        LongStream.range(0, numRowSets).parallel().mapToObj(keyRowSets::get)
                .sorted(Comparator.comparingLong(RowSet::firstRowKey)).forEachOrdered(rs -> {
                    builder.appendRowSequence(rs);
                    rs.close();
                });

        return builder.build();
    }

    @Override
    @NotNull
    public RowKeyLookup rowKeyLookup(final DataIndexOptions options) {
        table(options);
        return (final Object key, final boolean usePrev) -> {
            // Pass the object to the aggregation lookup, then return the resulting row position (which is also the row
            // key).
            final int keyRowPosition = lookupFunction.get(key);
            if (keyRowPosition == lookupFunction.noEntryValue()) {
                return RowSequence.NULL_ROW_KEY;
            }
            return keyRowPosition;
        };
    }

    @Override
    public boolean isRefreshing() {
        return false;
    }

    @Override
    public boolean isValid() {
        if (isCorrupt) {
            return false;
        }
        if (isValid != null) {
            return isValid;
        }
        final String[] keyColumnNamesArray = keyColumnNames.toArray(String[]::new);
        try (final CloseableIterator<TableLocation> locations =
                columnSourceManager.locationTable().objectColumnIterator(columnSourceManager.locationColumnName())) {
            return isValid = locations.stream().parallel().allMatch(l -> l.hasDataIndex(keyColumnNamesArray));
        }
    }

    @Override
    public boolean shouldRetain() {
        return true;
    }
}
