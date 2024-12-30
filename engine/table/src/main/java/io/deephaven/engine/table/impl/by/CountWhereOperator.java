//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;
import static io.deephaven.engine.util.TableTools.emptyTable;

/**
 * Implements a counting operator that counts the number of rows that pass a set of filters. Chunk data is accessed
 * through {@link RecordingInternalOperator recorder} instances.
 */

public class CountWhereOperator implements IterativeChunkedAggregationOperator {
    /**
     * The output column name in the result table.
     */
    private final String resultName;
    /**
     * The output column source in the result table.
     */
    private final LongArraySource resultColumnSource;
    /**
     * The filters to apply to the data.
     */
    private final CountFilter[] filters;
    /**
     * The recorder instances that provide the chunk data.
     */
    private final RecordingInternalOperator[] recorders;
    /**
     * A table constructed from chunk sources, populated with incoming chunk data and used to evaluate filters.
     */
    private final Table chunkSourceTable;
    /**
     * The chunk sources that populate the chunkSourceTable.
     */
    private final ChunkColumnSource<?>[] chunkColumnSources;
    /**
     * Track whether the chunkSourceTable needs to be updated with new data.
     */
    private final boolean updateChunkSourceTable;

    /**
     * Internal representation of the filter to apply to the data.
     */
    private static class CountFilter {
        private final ChunkFilter chunkFilter;
        private final AbstractConditionFilter.Filter conditionFilter;
        private final WhereFilter whereFilter;
        private final RecordingInternalOperator[] recorders;

        public CountFilter(ChunkFilter chunkFilter, RecordingInternalOperator[] recorders) {
            this.chunkFilter = chunkFilter;
            this.conditionFilter = null;
            this.whereFilter = null;
            this.recorders = recorders;
        }

        public CountFilter(AbstractConditionFilter.Filter conditionFilter, RecordingInternalOperator[] recorders) {
            this.chunkFilter = null;
            this.conditionFilter = conditionFilter;
            this.whereFilter = null;
            this.recorders = recorders;
        }

        public CountFilter(WhereFilter whereFilter, RecordingInternalOperator[] recorders) {
            this.chunkFilter = null;
            this.conditionFilter = null;
            this.whereFilter = whereFilter;
            this.recorders = recorders;
        }
    }

    static class BaseContext implements SafeCloseable {
        /**
         * ConditionFilter needs a context to store intermediate results.
         */
        final ConditionFilter.FilterKernel.Context[] conditionFilterContexts;
        /**
         * Contains the results of the filters as a boolean chunk, where true indicates a row passed all filters.
         */
        final WritableBooleanChunk<Values> resultsChunk;
        /**
         * The chunk data from the recorders to be used as input to the filters.
         */
        final Chunk<? extends Values>[][] filterChunks;

        BaseContext(final int size, final CountFilter[] filters) {
            conditionFilterContexts = new ConditionFilter.FilterKernel.Context[filters.length];
            for (int ii = 0; ii < filters.length; ii++) {
                if (filters[ii].conditionFilter != null) {
                    conditionFilterContexts[ii] = filters[ii].conditionFilter.getContext(size);
                }
            }
            resultsChunk = WritableBooleanChunk.makeWritableChunk(size);
            // noinspection unchecked
            filterChunks = new Chunk[filters.length][];
            for (int ii = 0; ii < filters.length; ii++) {
                // noinspection unchecked
                filterChunks[ii] = new Chunk[filters[ii].recorders.length];
            }
        }

        @Override
        public void close() {
            SafeCloseableArray.close(conditionFilterContexts);
            resultsChunk.close();
            // filterChunks are not owned by this context
        }
    }

    static class CountWhereBucketedContext extends BaseContext implements BucketedContext {
        /**
         * How many rows passed the filters for each destination.
         */
        final WritableIntChunk<Values> countChunk;
        /**
         * How many previous rows passed the filters for each destination, only applies to modified rows.
         */
        final WritableIntChunk<Values> previousCountChunk;

        private CountWhereBucketedContext(final int size, final CountFilter[] filters) {
            super(size, filters);
            countChunk = WritableIntChunk.makeWritableChunk(size);
            previousCountChunk = WritableIntChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            super.close();
            countChunk.close();
            previousCountChunk.close();
        }
    }

    static class CountWhereSingletonContext extends BaseContext implements SingletonContext {
        private CountWhereSingletonContext(final int size, final CountFilter[] filters) {
            super(size, filters);
        }
    }

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    CountWhereOperator(
            @NotNull final String resultName,
            final WhereFilter[] filters,
            final RecordingInternalOperator[] recorders,
            final RecordingInternalOperator[][] filterRecorders) {
        this.resultName = resultName;
        this.resultColumnSource = new LongArraySource();

        // Build a dummy table for use by generic WhereFilters and ConditionFilters
        if (recorders.length == 0) {
            chunkColumnSources = null;
            chunkSourceTable = emptyTable(0);
        } else {
            chunkColumnSources = new ChunkColumnSource[recorders.length];
            final Map<String, ColumnSource<?>> columnSourceMap = new HashMap<>();
            for (int ii = 0; ii < recorders.length; ii++) {
                final ColumnSource<?> recorderSource = recorders[ii].getInputColumnSource();
                chunkColumnSources[ii] = ChunkColumnSource.make(recorderSource.getChunkType(), recorderSource.getType(),
                        recorderSource.getComponentType());
                columnSourceMap.put(recorders[ii].getInputColumnName(), chunkColumnSources[ii]);
            }
            chunkSourceTable = new QueryTable(RowSetFactory.empty().toTracking(), columnSourceMap);
        }

        // Create the internal filters
        final List<CountFilter> filterList = new ArrayList<>();
        boolean forcedWhereFilter = false;
        for (int fi = 0; fi < filters.length; fi++) {
            final WhereFilter filter = filters[fi];
            final CountWhereOperator.CountFilter countFilter;
            if (!forcedWhereFilter && filter instanceof ConditionFilter) {
                final ConditionFilter conditionFilter = (ConditionFilter) filter;
                if (conditionFilter.hasVirtualRowVariables()) {
                    throw new UnsupportedOperationException("AggCountWhere does not support refreshing filters");
                }
                try {
                    countFilter = new CountWhereOperator.CountFilter(
                            conditionFilter.getFilter(chunkSourceTable, RowSetFactory.empty()),
                            filterRecorders[fi]);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("Error creating condition filter in CountWhereOperator", e);
                }
            } else if (!forcedWhereFilter && filter instanceof ExposesChunkFilter
                    && ((ExposesChunkFilter) filter).chunkFilter().isPresent()) {
                final Optional<ChunkFilter> chunkFilter = ((ExposesChunkFilter) filter).chunkFilter();
                countFilter = new CountWhereOperator.CountFilter(chunkFilter.get(), filterRecorders[fi]);
            } else {
                try (final SafeCloseable ignored = filter.beginOperation(chunkSourceTable)) {
                    countFilter = new CountWhereOperator.CountFilter(filter, filterRecorders[fi]);
                }
                forcedWhereFilter = true;
            }
            filterList.add(countFilter);
        }

        this.updateChunkSourceTable = forcedWhereFilter;
        this.filters = filterList.toArray(CountFilter[]::new);
        this.recorders = recorders;
    }

    /**
     * Count the number of rows that passed the filters in the given chunk.
     */
    private static int countChunk(final BooleanChunk<Values> values, final int start, final int len) {
        int count = 0;
        for (int ii = start; ii < start + len; ii++) {
            if (values.get(ii)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Build a RowSet from a boolean chunk, including only the indices that are true.
     */
    private static WritableRowSet buildFromBooleanChunk(final BooleanChunk<Values> values, final int chunkSize) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (int ii = 0; ii < chunkSize; ii++) {
            if (values.get(ii)) {
                builder.appendKey(ii);
            }
        }
        return builder.build();
    }

    /**
     * Apply the filters to the data in the chunk, returning the number of rows that passed the filters. If
     * {@code requiresPopulatedResultsChunk == true}, the results chunk will be populated with {@code true} for every
     * row that passed the filters.
     */
    private int applyFilters(
            final BaseContext ctx,
            final int chunkSize,
            final boolean requiresPopulatedResultsChunk) {

        boolean initialized = false;
        WritableRowSet remainingRows = null;
        final RowSet flatRowSet = RowSetFactory.flat(chunkSize);

        int count = 0;

        // We must apply the filters in the order they were given.
        for (int fi = 0; fi < filters.length; fi++) {
            final CountFilter filter = filters[fi];
            final Chunk<? extends Values>[] valueChunks = ctx.filterChunks[fi];
            final ConditionFilter.FilterKernel.Context conditionalFilterContext = ctx.conditionFilterContexts[fi];

            if (filter.chunkFilter != null) {
                if (!initialized) {
                    // Chunk filters only have one input.
                    count = filter.chunkFilter.filter(valueChunks[0], ctx.resultsChunk);
                    initialized = true;
                } else {
                    count = filter.chunkFilter.filterAnd(valueChunks[0], ctx.resultsChunk);
                }
                continue;
            } else if (filter.conditionFilter != null) {
                if (!initialized) {
                    count = filter.conditionFilter.filter(conditionalFilterContext, valueChunks,
                            chunkSize, ctx.resultsChunk);
                    initialized = true;
                } else {
                    count = filter.conditionFilter.filterAnd(conditionalFilterContext, valueChunks,
                            chunkSize, ctx.resultsChunk);
                }
                continue;
            }

            if (remainingRows == null) {
                // This is the first WhereFilter to run, initialize the remainingRows RowSet
                remainingRows = initialized
                        ? buildFromBooleanChunk(ctx.resultsChunk, chunkSize)
                        : RowSetFactory.flat(chunkSize);
            }
            try (final RowSet ignored = remainingRows) {
                remainingRows = filter.whereFilter.filter(remainingRows, flatRowSet, chunkSourceTable, false);
            }
            initialized = true;
        }

        try (final RowSet ignored = remainingRows; final RowSet ignored2 = flatRowSet) {
            if (remainingRows != null) {
                // WhereFilters were used, so gather the info from remainingRows
                if (requiresPopulatedResultsChunk) {
                    ctx.resultsChunk.fillWithValue(0, chunkSize, false);
                    remainingRows.forAllRowKeyRanges(
                            (start, end) -> ctx.resultsChunk.fillWithValue((int) start, (int) end - (int) start + 1,
                                    true));
                }
                return remainingRows.intSize();
            }
        }
        return count;
    }

    /**
     * Using current data, update the provided context to contain the chunks from the recording operators and update the
     * chunk source table if needed.
     */
    private void updateChunkSources(final BaseContext ctx, final boolean usePrev) {
        if (updateChunkSourceTable) {
            for (int ii = 0; ii < chunkColumnSources.length; ii++) {
                chunkColumnSources[ii].clear();
                final Chunk<? extends Values> chunk = usePrev
                        ? recorders[ii].getPrevValueChunk()
                        : recorders[ii].getValueChunk();
                // ChunkColumnSource releases the chunks it acquires, so give it a copy.
                final WritableChunk<? extends Values> tmpValues =
                        (WritableChunk<? extends Values>) chunk.slice(0, chunk.size());
                chunkColumnSources[ii].addChunk(tmpValues);
            }
        }

        // Grab the filter input chunks from the recorders
        for (int fi = 0; fi < filters.length; fi++) {
            for (int ri = 0; ri < filters[fi].recorders.length; ri++) {
                final Chunk<? extends Values> chunk = usePrev
                        ? filters[fi].recorders[ri].getPrevValueChunk()
                        : filters[fi].recorders[ri].getValueChunk();
                ctx.filterChunks[fi][ri] = chunk;
            }
        }
    }

    /**
     * Count the number of rows for each destination that pass the filters and store into the {@code destCountChunk}.
     */
    private void doCountBucketed(
            final CountWhereBucketedContext ctx,
            final int chunkSize,
            final IntChunk<RowKeys> destinations,
            final IntChunk<ChunkPositions> startPositions,
            final IntChunk<ChunkLengths> length,
            final WritableIntChunk<Values> destCountChunk,
            final boolean usePrev) {

        updateChunkSources(ctx, usePrev);

        applyFilters(ctx, chunkSize, true);

        // fill the destination count chunk with the number of rows that passed the filter
        for (int dest = 0; dest < startPositions.size(); dest++) {
            final int start = startPositions.get(dest);
            final int len = length.get(dest);
            destCountChunk.set(dest, countChunk(ctx.resultsChunk, start, len));
        }
    }

    /**
     * Count the number of rows that pass the filters..
     */
    private int doCountSingleton(
            final CountWhereSingletonContext ctx,
            final int chunkSize,
            final boolean usePrev) {
        updateChunkSources(ctx, usePrev);

        return applyFilters(ctx, chunkSize, false);
    }

    @Override
    public void addChunk(
            BucketedContext context,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final CountWhereBucketedContext ctx = (CountWhereBucketedContext) context;

        // Compute the chunk size from the startPositions and length
        final int chunkSize = startPositions.get(startPositions.size() - 1) + length.get(length.size() - 1);

        // Do the counting work and store the results in the count chunk
        doCountBucketed(ctx, chunkSize, destinations, startPositions, length, ctx.countChunk, false);

        // Update the result column source with the counts for each destination
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int count = ctx.countChunk.get(ii);
            final long destination = destinations.get(startPosition);
            resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), count));
            stateModified.set(ii, true);
        }
    }

    @Override
    public boolean addChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        if (chunkSize == 0) {
            return false;
        }

        final CountWhereSingletonContext ctx = (CountWhereSingletonContext) context;
        final int count = doCountSingleton(ctx, chunkSize, false);
        resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), count));
        return count > 0;
    }

    @Override
    public void removeChunk(
            BucketedContext context,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final CountWhereBucketedContext ctx = (CountWhereBucketedContext) context;

        // Compute the chunk size from the startPositions and length
        final int chunkSize = startPositions.get(startPositions.size() - 1) + length.get(length.size() - 1);

        // Do the counting work and store the results in the count chunk
        doCountBucketed(ctx, chunkSize, destinations, startPositions, length, ctx.countChunk, false);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int count = ctx.countChunk.get(ii);
            if (count > 0) {
                final int startPosition = startPositions.get(ii);
                final long destination = destinations.get(startPosition);
                final long updatedCount = plusLong(resultColumnSource.getUnsafe(destination), -count);

                Assert.geqZero(updatedCount, "updatedCount");
                resultColumnSource.set(destination, updatedCount);
                stateModified.set(ii, true);
            } else {
                stateModified.set(ii, false);
            }
        }
    }

    @Override
    public boolean removeChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            long destination) {
        if (chunkSize == 0) {
            return false;
        }

        final CountWhereSingletonContext ctx = (CountWhereSingletonContext) context;
        final int count = doCountSingleton(ctx, chunkSize, false);
        if (count > 0) {
            final long updatedCount = plusLong(resultColumnSource.getUnsafe(destination), -count);
            Assert.geqZero(updatedCount, "updatedCount");
            resultColumnSource.set(destination, updatedCount);
            return true;
        }
        return false;
    }

    @Override
    public void modifyChunk(
            BucketedContext context,
            Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final CountWhereBucketedContext ctx = (CountWhereBucketedContext) context;

        // Compute the chunk size from the startPositions and length
        final int chunkSize = startPositions.get(startPositions.size() - 1) + length.get(length.size() - 1);

        // Fill the context with previous chunk data
        doCountBucketed(ctx, chunkSize, destinations, startPositions, length, ctx.previousCountChunk, true);
        // Fill the context with current chunk data
        doCountBucketed(ctx, chunkSize, destinations, startPositions, length, ctx.countChunk, false);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int oldCount = ctx.previousCountChunk.get(ii);
            final int newCount = ctx.countChunk.get(ii);
            final int count = newCount - oldCount;
            if (count != 0) {
                final int startPosition = startPositions.get(ii);
                final long destination = destinations.get(startPosition);
                final long updatedCount = plusLong(resultColumnSource.getUnsafe(destination), count);
                Assert.geqZero(updatedCount, "updatedCount");
                resultColumnSource.set(destination, updatedCount);
                stateModified.set(ii, true);
            } else {
                stateModified.set(ii, false);
            }
        }
    }

    @Override
    public boolean modifyChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends RowKeys> postShiftRowKeys,
            long destination) {
        if (chunkSize == 0) {
            return false;
        }

        final CountWhereSingletonContext ctx = (CountWhereSingletonContext) context;
        final int prevCount = doCountSingleton(ctx, chunkSize, true);
        final int newCount = doCountSingleton(ctx, chunkSize, false);

        final int count = newCount - prevCount;
        if (count != 0) {
            final long updatedCount = plusLong(resultColumnSource.getUnsafe(destination), count);
            Assert.geqZero(updatedCount, "updatedCount");
            resultColumnSource.set(destination, updatedCount);
            return true;
        }
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumnSource.ensureCapacity(tableSize, false);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(resultName, resultColumnSource);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumnSource.startTrackingPrevValues();
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new CountWhereBucketedContext(size, filters);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new CountWhereSingletonContext(size, filters);
    }
}
