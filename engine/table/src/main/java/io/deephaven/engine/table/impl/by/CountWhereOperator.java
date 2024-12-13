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
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;
import static io.deephaven.engine.util.TableTools.emptyTable;

public class CountWhereOperator implements IterativeChunkedAggregationOperator {
    private static class InternalFilter {
        private final ChunkFilter chunkFilter;
        private final AbstractConditionFilter.Filter conditionFilter;
        private final WhereFilter whereFilter;

        public InternalFilter(ChunkFilter chunkFilter) {
            this.chunkFilter = chunkFilter;
            this.conditionFilter = null;
            this.whereFilter = null;
        }

        public InternalFilter(AbstractConditionFilter.Filter conditionFilter) {
            this.chunkFilter = null;
            this.conditionFilter = conditionFilter;
            this.whereFilter = null;
        }

        public InternalFilter(WhereFilter whereFilter) {
            this.chunkFilter = null;
            this.conditionFilter = null;
            this.whereFilter = whereFilter;
        }
    }

    private final String resultName;
    private final LongArraySource resultColumnSource;
    private final InternalFilter[] internalFilters;

    /**
     * A table constructed from chunk sources, will be populated with incoming chunk data and used to evaluate filters.
     */
    final Table chunkSourceTable;
    final ChunkColumnSource<?> chunkColumnSource;

    static class CountWhereBucketedContext implements BucketedContext {
        final ConditionFilter.FilterKernel.Context[] conditionFilterContexts;
        final WritableBooleanChunk<Values> resultsChunk;
        final WritableIntChunk<Values> countChunk;
        // extra chunk for comparing previous values
        final WritableIntChunk<Values> previousCountChunk;

        private CountWhereBucketedContext(final int size, final InternalFilter[] filters) {
            conditionFilterContexts = new ConditionFilter.FilterKernel.Context[filters.length];
            for (int ii = 0; ii < filters.length; ii++) {
                if (filters[ii].conditionFilter != null) {
                    conditionFilterContexts[ii] = filters[ii].conditionFilter.getContext(size);
                }
            }
            resultsChunk = WritableBooleanChunk.makeWritableChunk(size);
            countChunk = WritableIntChunk.makeWritableChunk(size);
            previousCountChunk = WritableIntChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            resultsChunk.close();
            countChunk.close();
            previousCountChunk.close();
            SafeCloseableArray.close(conditionFilterContexts);
        }
    }

    static class CountWhereSingletonContext implements SingletonContext {
        final ConditionFilter.FilterKernel.Context[] conditionFilterContexts;
        final WritableBooleanChunk<Values> resultsChunk;

        private CountWhereSingletonContext(final int size, final InternalFilter[] filters) {
            conditionFilterContexts = new ConditionFilter.FilterKernel.Context[filters.length];
            for (int ii = 0; ii < filters.length; ii++) {
                if (filters[ii].conditionFilter != null) {
                    conditionFilterContexts[ii] = filters[ii].conditionFilter.getContext(size);
                }
            }
            resultsChunk = WritableBooleanChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            resultsChunk.close();
            SafeCloseableArray.close(conditionFilterContexts);
        }
    }

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    CountWhereOperator(
            @NotNull final String resultName,
            final WhereFilter[] inputFilters,
            @Nullable final String inputColumnName,
            @Nullable final ColumnSource<?> inputSource) {
        this.resultName = resultName;
        this.resultColumnSource = new LongArraySource();

        // Build a dummy table for use by generic WhereFilters and ConditionFilters
        if (inputColumnName == null || inputSource == null) {
            chunkColumnSource = null;
            chunkSourceTable = emptyTable(0);
        } else {
            chunkColumnSource = ChunkColumnSource.make(inputSource.getChunkType(), inputSource.getType(),
                    inputSource.getComponentType());
            chunkSourceTable = new QueryTable(RowSetFactory.empty().toTracking(),
                    Collections.singletonMap(inputColumnName, chunkColumnSource));
        }

        final List<InternalFilter> internalFilters = new ArrayList<>();

        // sort out the types of filters
        boolean forceWhereFilter = false;
        for (final WhereFilter filter : inputFilters) {
            final InternalFilter internalFilter;
            if (forceWhereFilter) {
                internalFilter = new InternalFilter(filter);
            } else if (filter instanceof ConditionFilter) {
                try {
                    internalFilter = new InternalFilter(
                            ((ConditionFilter) filter).getFilter(chunkSourceTable, chunkSourceTable.getRowSet()));
                } catch (final Exception e) {
                    throw new IllegalArgumentException("Error creating condition filter in CountWhereOperator", e);
                }
            } else if (filter instanceof ExposesChunkFilter) {
                final Optional<ChunkFilter> chunkFilter = ((ExposesChunkFilter) filter).chunkFilter();
                if (chunkFilter.isPresent()) {
                    internalFilter = new InternalFilter(chunkFilter.get());
                } else {
                    internalFilter = new InternalFilter(filter);
                    forceWhereFilter = true;
                }
            } else {
                internalFilter = new InternalFilter(filter);
                forceWhereFilter = true;
            }
            internalFilters.add(internalFilter);
        }
        this.internalFilters = internalFilters.toArray(InternalFilter[]::new);
    }

    private static int countChunk(final BooleanChunk<Values> values, final int start, final int len) {
        int count = 0;
        for (int ii = start; ii < start + len; ii++) {
            if (values.get(ii)) {
                count++;
            }
        }
        return count;
    }

    private static WritableRowSet buildFromBooleanChunk(final BooleanChunk<Values> values, final int chunkSize) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (int ii = 0; ii < chunkSize; ii++) {
            if (values.get(ii)) {
                builder.appendKey(ii);
            }
        }
        return builder.build();
    }

    private int applyFilters(
            final Chunk<? extends Values> values,
            final int chunkSize,
            final WritableBooleanChunk<Values> resultsChunk,
            final boolean requiresPopulatedResultsChunk,
            final ConditionFilter.FilterKernel.Context[] conditionalFilterContexts) {

        boolean initialized = false;
        WritableRowSet remainingRows = null;
        final RowSet flatRowSet = RowSetFactory.flat(chunkSize);

        int count = 0;

        // We must apply the filters in the order they were given.
        for (int ii = 0; ii < internalFilters.length; ii++) {
            final InternalFilter filter = internalFilters[ii];
            if (filter.chunkFilter != null) {
                if (!initialized) {
                    count = filter.chunkFilter.filter(values, resultsChunk);
                    initialized = true;
                } else {
                    // Decrement the count by the number of false values written
                    count -= filter.chunkFilter.filterAnd(values, resultsChunk);
                }
                continue;
            } else if (filter.conditionFilter != null) {
                if (!initialized) {
                    count = filter.conditionFilter.filter(conditionalFilterContexts[ii], new Chunk[] {values},
                            chunkSize, resultsChunk);
                    initialized = true;
                } else {
                    // Decrement the count by the number of false values written
                    count -= filter.conditionFilter.filterAnd(conditionalFilterContexts[0], new Chunk[] {values},
                            chunkSize, resultsChunk);
                }
                continue;
            }
            if (remainingRows == null) {
                // This is the first WhereFilter to run
                remainingRows = initialized
                        ? buildFromBooleanChunk(resultsChunk, chunkSize)
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
                    resultsChunk.fillWithValue(0, chunkSize, false);
                    remainingRows.forAllRowKeyRanges(
                            (start, end) -> resultsChunk.fillWithValue((int) start, (int) end - (int) start + 1, true));
                }
                return remainingRows.intSize();
            }
        }
        return count;
    }

    /**
     * Given the data chunk and multiple destinations, count the number of rows that pass the filters and store into the
     * count chunk.
     */
    private void doCountBucketed(
            final CountWhereBucketedContext ctx,
            final Chunk<? extends Values> values,
            final IntChunk<RowKeys> destinations,
            final IntChunk<ChunkPositions> startPositions,
            final IntChunk<ChunkLengths> length,
            final WritableIntChunk<Values> destCountChunk) {
        if (chunkColumnSource != null) {
            // Clear the chunk sources and add the new chunk
            chunkColumnSource.clear();
            // ChunkColumnSource releases the chunks it acquires, so give it a copy.
            final WritableChunk<? extends Values> tmpValues =
                    (WritableChunk<? extends Values>) values.slice(0, values.size());
            chunkColumnSource.addChunk(tmpValues);
        }

        // if values is null, so get the chunk size from the startPositions and length
        final int chunkSize = values != null ? values.size()
                : startPositions.get(startPositions.size() - 1) + length.get(length.size() - 1);

        applyFilters(values, chunkSize, ctx.resultsChunk, true, ctx.conditionFilterContexts);

        // fill the destination count chunk with the number of rows that passed the filter
        for (int dest = 0; dest < startPositions.size(); dest++) {
            final int start = startPositions.get(dest);
            final int len = length.get(dest);
            destCountChunk.set(dest, countChunk(ctx.resultsChunk, start, len));
        }
    }

    private int doCountSingleton(
            final CountWhereSingletonContext ctx,
            final Chunk<? extends Values> values,
            final int chunkSize) {
        if (chunkSize == 0) {
            return 0;
        }

        if (chunkColumnSource != null) {
            // Clear the chunk sources and add the new chunk
            chunkColumnSource.clear();
            // ChunkColumnSource releases the chunks it acquires, so give it a copy.
            final WritableChunk<? extends Values> tmpValues =
                    (WritableChunk<? extends Values>) values.slice(0, chunkSize);
            chunkColumnSource.addChunk(tmpValues);
        }

        return applyFilters(values, chunkSize, ctx.resultsChunk, false, ctx.conditionFilterContexts);
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

        // Do the counting work and store the results in the count chunk
        doCountBucketed(ctx, values, destinations, startPositions, length, ctx.countChunk);

        // Update the result column source with the counts
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int count = ctx.countChunk.get(ii);
            final long destination = destinations.get(startPosition);
            resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), count));
            stateModified.set(ii, count > 0);
        }
    }

    @Override
    public boolean addChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final CountWhereSingletonContext ctx = (CountWhereSingletonContext) context;

        final int count = doCountSingleton(ctx, values, chunkSize);
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

        // Do the counting work and store the results in the count chunk
        doCountBucketed(ctx, values, destinations, startPositions, length, ctx.countChunk);

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
        final CountWhereSingletonContext ctx = (CountWhereSingletonContext) context;

        final int count = doCountSingleton(ctx, values, chunkSize);
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

        // Do the counting work and store the results in the count chunk
        doCountBucketed(ctx, previousValues, destinations, startPositions, length, ctx.previousCountChunk);
        doCountBucketed(ctx, newValues, destinations, startPositions, length, ctx.countChunk);

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
        final CountWhereSingletonContext ctx = (CountWhereSingletonContext) context;

        final int oldCount = doCountSingleton(ctx, previousValues, chunkSize);
        final int newCount = doCountSingleton(ctx, newValues, chunkSize);
        final int count = newCount - oldCount;
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
        return new CountWhereBucketedContext(size, internalFilters);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new CountWhereSingletonContext(size, internalFilters);
    }
}
