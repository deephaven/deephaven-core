/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.GroupingBuilder;
import io.deephaven.engine.table.impl.locations.GroupingBuilderFactory;
import io.deephaven.engine.table.impl.locations.KeyRangeGroupingProvider;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Grouping provider that loads column location metadata and assembles grouping indexes lazily on demand.
 */
public class DiskBackedDeferredGroupingProvider<DATA_TYPE> extends MemoizingGroupingProvider
        implements KeyRangeGroupingProvider {
    private static final class NoParallelWhereThread extends ForkJoinWorkerThread {
        /**
         * Creates a ForkJoinWorkerThread operating in the given pool.
         *
         * @param pool the pool this thread works in
         * @throws NullPointerException if pool is null
         */
        private NoParallelWhereThread(ForkJoinPool pool) {
            super(pool);
        }

        @Override
        protected void onStart() {
            QueryTable.disableParallelWhereForThread();
        }
    }

    public static final int THREADPOOL_SIZE =
            Configuration.getInstance().getIntegerWithDefault("ParallelDeferredGroupingProvider.threadPoolSize", 32);
    public static final ForkJoinPool GROUPING_POOL =
            new ForkJoinPool(THREADPOOL_SIZE, NoParallelWhereThread::new, null, false);
    private static final int CHUNK_SIZE = 2048;

    private final ColumnDefinition<DATA_TYPE> columnDefinition;
    private final Logger log;
    private final List<LocationState> sources = new ArrayList<>();

    /**
     * The base implementation of {@link LocationState}
     */
    private class LocationState {
        final ColumnLocation columnLocation;

        @NotNull
        final RowSet rowSet;

        /**
         * We maintain a reference to the resultant grouping information. This allows us to avoid re-reading grouping
         * information.
         * <p>
         * The reference is soft to avoid having the cache result in an OOM.
         */
        private SoftReference<Table> cachedGrouping;

        private LocationState(@NotNull final ColumnLocation columnLocation, @NotNull RowSet locationRowSetInTable) {
            Require.eqFalse(locationRowSetInTable.isEmpty(), "locationRowSetInTable.isEmpty(");
            this.columnLocation = Require.neqNull(columnLocation, "columnLocation");
            this.rowSet = locationRowSetInTable.copy();
        }

        public boolean overlaps(@NotNull final RowSet rowSetOfInterest) {
            return rowSetOfInterest.overlaps(rowSet);
        }

        /**
         * Load the grouping data for this location. This will use the checkpoint version to determine if the grouping
         * is stored as a legacy hashmap in the column metadata, or if it is in adjacent tables per source.
         * <p>
         * If the grouping information was already read, get it from the cached results map. Any grouping information
         * read by this method will be stored in the cached results map.
         *
         * @return Grouping metadata as a map from value to position range within this source, or null if the grouping
         *         information was not present
         */
        public Table getGroupingTable() {
            if (!columnLocation.exists()) {
                // TODO: is this a copy or can we pass the actual set?
                return GroupingBuilderFactory.makeNullOnlyGroupingTable(columnDefinition, rowSet.copy());
            }

            // Check and return the cached result if we already have one.
            Table groupingTable = getCachedGrouping();
            if (groupingTable != null) {
                return groupingTable;
            }

            synchronized (this) {
                groupingTable = getCachedGrouping();
                if (groupingTable != null) {
                    return groupingTable;
                }

                final TableLocation tableLoc = columnLocation.getTableLocation();
                // isGrouping must be checked first to avoid the expensive hasDataIndexFor call (which reads the table's
                // size file)
                if (columnDefinition.isGrouping() || tableLoc.hasDataIndexFor(columnDefinition.getName())) {
                    // TODO: Make sure that we are converting the old column name to the new column in the below
                    groupingTable = columnLocation.getTableLocation().getDataIndex(columnDefinition.getName());
                    if (groupingTable != null) {
                        groupingTable =
                                groupingTable.update("Index=(io.deephaven.engine.rowset.WritableRowSet)Index.shift("
                                        + rowSet.firstRowKey() + ")");
                    }
                }

                validateGrouping(groupingTable);
                cachedGrouping = new SoftReference<>(groupingTable);
            }

            return groupingTable;
        }

        /**
         * Validate the grouping for this source. The size of the grouping should be the same as the size of the range
         * or there are unaccounted for rows.
         *
         * @param grouping The grouping as a map from value to position range within this source
         */
        private void validateGrouping(@Nullable final Table grouping) {
            if (grouping == null) {
                return;
            }

            final long locationSize = rowSet.size();
            if (grouping.isEmpty()) {
                // NB: It's impossible for the location to be legitimately empty, since the constructor validates that
                // firstKey <= lastKey.
                throw new IllegalStateException(
                        "Invalid empty grouping for " + columnLocation + ": expected " + locationSize + " rows");
            }

            long totalRangeSize = 0;
            // noinspection unchecked
            final ColumnSource<RowSet> indexSource = grouping.getColumnSource(INDEX_COL_NAME);
            final int chunkSize = Math.min(CHUNK_SIZE, grouping.intSize());
            try (final ChunkSource.GetContext indexGetContext = indexSource.makeGetContext(chunkSize);
                    final RowSequence.Iterator okIt = grouping.getRowSet().getRowSequenceIterator()) {

                while (okIt.hasMore()) {
                    final RowSequence nextKeys = okIt.getNextRowSequenceWithLength(chunkSize);
                    final ObjectChunk<RowSet, ? extends Values> indexChunk =
                            indexSource.getChunk(indexGetContext, nextKeys).asObjectChunk();

                    for (int ii = 0; ii < nextKeys.size(); ii++) {
                        totalRangeSize += indexChunk.get(ii).size();
                    }
                }
            }

            if (locationSize != totalRangeSize) {
                throw new IllegalStateException("Invalid grouping for " + columnLocation + ": found " + totalRangeSize
                        + " rows, but expected " + locationSize);
            }
        }

        @Nullable
        private Table getCachedGrouping() {
            return cachedGrouping == null ? null : cachedGrouping.get();
        }
    }

    public DiskBackedDeferredGroupingProvider(@NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final Logger log) {
        this.columnDefinition = columnDefinition;
        this.log = log;
    }

    @Override
    public void addSource(@NotNull final ColumnLocation columnLocation, @NotNull RowSet locationRowSetInTable) {
        sources.add(new LocationState(columnLocation, locationRowSetInTable));
        clearMemoizedGroupings();
    }

    @NotNull
    @Override
    public GroupingBuilder getGroupingBuilder() {
        return new ProviderGroupingBuilder();
    }

    @Override
    public boolean hasGrouping() {
        if (sources.get(0).getGroupingTable() == null) {
            return false;
        }

        try {
            return GROUPING_POOL.submit(() -> sources.parallelStream().noneMatch(ls -> ls.getGroupingTable() == null))
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }

    private class ProviderGroupingBuilder extends AbstractGroupingBuilder {
        @Override
        public long estimateGroupingSize() {
            final List<Table> locationIndexes = collectRelevantLocations();
            if (locationIndexes == null || locationIndexes.isEmpty()) {
                return -1;
            }

            final Table result = TableTools.merge(locationIndexes).selectDistinct(columnDefinition.getName());
            return result.size();
        }

        @NotNull
        @Override
        public String getValueColumnName() {
            return columnDefinition.getName();
        }

        @Override
        public Table buildTable() {
            // TODO: This is not a great solution to the problem at hand. The problem is that you can exhaust the where
            // threadpool
            // and end up deadlocked because of the nested call to where() that this might invoke. The most obvious case
            // of this
            // is when FORCE_PARALLEL_WHERE is set and we are in unit test mode. In that case threadpool.size == 1 and
            // we immediately
            // end up consuming the single executor, then attempt to call it again here but are then stuck forever.
            // We need to come up with a better way around this problem.
            try (final SafeCloseable ignored = QueryTable.disableParallelWhereForThread()) {
                return memoizeGrouping(makeMemoKey(), this::doBuildTable);
            }
        }

        private Table doBuildTable() {
            return QueryPerformanceRecorder.withNugget("Build Grouping:", () -> {
                final List<Table> locationIndexes =
                        QueryPerformanceRecorder.withNugget("Collect Location Tables:", this::collectRelevantLocations);

                // Bail out early if we couldn't get groupings for each location.
                if (locationIndexes == null || locationIndexes.isEmpty()) {
                    return null;
                }

                final List<Table> mutatedLocationIndexes =
                        QueryPerformanceRecorder.withNugget("Apply Location Mutators:", () -> {
                            if (groupKeys == null && regionMutators.isEmpty()) {
                                return locationIndexes;
                            }

                            try {
                                return GROUPING_POOL.submit(() -> locationIndexes.parallelStream()
                                        .map(locationIndex -> {
                                            locationIndex = maybeApplyMatch(locationIndex);
                                            for (final Function<Table, Table> regionMutator : regionMutators) {
                                                locationIndex = regionMutator.apply(locationIndex);
                                                if (locationIndex == null || locationIndex.isEmpty()) {
                                                    return null;
                                                }
                                            }

                                            return locationIndex;
                                        })
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList())).get();
                            } catch (InterruptedException | ExecutionException e) {
                                log.warn().append("Failed to mutate grouping for ").append(columnDefinition)
                                        .append(": ").append(e).endl();
                            }
                            return null;
                        });

                // Bail out early if mutation ended up eliminating everything
                if (mutatedLocationIndexes.isEmpty()) {
                    return null;
                }

                return QueryPerformanceRecorder.withNugget("Merge Mutated Location Tables:", () -> {
                    final Table result = condenseGrouping(TableTools.merge(mutatedLocationIndexes));
                    return maybeSortByFirsKey(applyIntersectAndInvert(result));
                });
            });
        }

        @Nullable
        private List<Table> collectRelevantLocations() {
            final List<LocationState> relevantStates = rowSetOfInterest == null ? sources
                    : sources.stream().filter(ls -> ls.overlaps(rowSetOfInterest)).collect(Collectors.toList());
            if (relevantStates.isEmpty()) {
                return null;
            }

            final Table firstSourceGrouping = relevantStates.get(0).getGroupingTable();
            if (firstSourceGrouping == null) {
                return null;
            }

            final List<Table> tempLocationIndexes = new ArrayList<>();
            tempLocationIndexes.add(firstSourceGrouping);

            try {
                GROUPING_POOL.submit(() -> relevantStates.subList(1, relevantStates.size())
                        .parallelStream()
                        .map(LocationState::getGroupingTable)
                        .forEachOrdered(grouping -> {
                            synchronized (tempLocationIndexes) {
                                tempLocationIndexes.add(grouping);
                            }
                        })).get();
            } catch (InterruptedException | ExecutionException e) {
                log.warn().append("Failed to load grouping for ").append(columnDefinition).append(": ").append(e)
                        .endl();
                return null;
            }

            if (tempLocationIndexes.stream().anyMatch(Objects::isNull)) {
                return null;
            }

            return tempLocationIndexes;
        }
    }
}
