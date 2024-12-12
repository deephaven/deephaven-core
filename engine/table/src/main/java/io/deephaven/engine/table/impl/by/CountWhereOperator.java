//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.AbstractConditionFilter;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.ExposesChunkFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;
import static io.deephaven.engine.util.TableTools.emptyTable;

public class CountWhereOperator implements IterativeChunkedAggregationOperator {
    private final String resultName;
    private final LongArraySource resultColumnSource;
    private final WhereFilter[] whereFilters;
    private final ChunkFilter[] chunkFilters;
    private final AbstractConditionFilter.Filter[] conditionFilters;

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

        public CountWhereBucketedContext(final int size, final AbstractConditionFilter.Filter[] conditionFilters) {
            if (conditionFilters.length > 0) {
                conditionFilterContexts = new ConditionFilter.FilterKernel.Context[conditionFilters.length];
                for (int ii = 0; ii < conditionFilters.length; ii++) {
                    conditionFilterContexts[ii] = conditionFilters[ii].getContext(size);
                }
            } else {
                conditionFilterContexts = null;
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
            if (conditionFilterContexts != null) {
                SafeCloseableArray.close(conditionFilterContexts);
            }
        }
    }

    static class CountWhereSingletonContext implements SingletonContext {
        final ConditionFilter.FilterKernel.Context[] conditionFilterContexts;
        final WritableBooleanChunk<Values> resultsChunk;

        public CountWhereSingletonContext(final int size, final AbstractConditionFilter.Filter[] conditionFilters) {
            if (conditionFilters.length > 0) {
                conditionFilterContexts = new ConditionFilter.FilterKernel.Context[conditionFilters.length];
                for (int ii = 0; ii < conditionFilters.length; ii++) {
                    conditionFilterContexts[ii] = conditionFilters[ii].getContext(size);
                }
            } else {
                conditionFilterContexts = null;
            }
            resultsChunk = WritableBooleanChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            resultsChunk.close();
            if (conditionFilterContexts != null) {
                SafeCloseableArray.close(conditionFilterContexts);
            }
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

        final List<ChunkFilter> chunkFilters = new ArrayList<>();
        final List<AbstractConditionFilter.Filter> conditionFilters = new ArrayList<>();
        final List<WhereFilter> whereFilters = new ArrayList<>();

        // sort out the types of filters
        for (final WhereFilter filter : inputFilters) {
            if (filter instanceof ConditionFilter) {
                try {
                    conditionFilters
                            .add(((ConditionFilter) filter).getFilter(chunkSourceTable, chunkSourceTable.getRowSet()));
                } catch (final Exception e) {
                    throw new IllegalArgumentException("Error creating condition filter in CountWhereOperator", e);
                }
            } else if (filter instanceof ExposesChunkFilter) {
                final Optional<ChunkFilter> chunkFilter = ((ExposesChunkFilter) filter).chunkFilter();
                if (chunkFilter.isPresent()) {
                    chunkFilters.add(chunkFilter.get());
                } else {
                    whereFilters.add(filter);
                }
            } else {
                whereFilters.add(filter);
            }
        }

        this.conditionFilters = conditionFilters.toArray(AbstractConditionFilter.Filter[]::new);
        this.chunkFilters = chunkFilters.toArray(ChunkFilter[]::new);
        this.whereFilters = whereFilters.toArray(WhereFilter[]::new);
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

    private int applyChunkedAndConditionFilters(
            final Chunk<? extends Values> values,
            final int chunkSize,
            final WritableBooleanChunk<Values> resultsChunk,
            final ConditionFilter.FilterKernel.Context[] conditionalFilterContexts) {
        // Pre-fill fill TRUE since the filtering will only set FALSE
        resultsChunk.fillWithValue(0, chunkSize, true);

        int count = 0;
        // Apply the chunk filters and keep a count of the number of rows that fail
        for (int ii = 0; ii < chunkFilters.length; ii++) {
            final ChunkFilter filter = chunkFilters[ii];
            count += filter.filter(values, resultsChunk);
        }

        // Apply the condition filters and keep a count of the number of rows that fail
        final Chunk<? extends Values>[] valueChunks = new Chunk[] {values};
        for (int ii = 0; ii < conditionFilters.length; ii++) {
            final ConditionFilter.FilterKernel.Context context = conditionalFilterContexts[ii];
            count += conditionFilters[ii].filter(context, valueChunks, chunkSize, resultsChunk);
        }
        return chunkSize - count;
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
        // if values is null, so get the chunk size from the startPositions and length
        final int chunkSize = values != null ? values.size()
                : startPositions.get(startPositions.size() - 1) + length.get(length.size() - 1);

        final RowSet initialRows;
        if (chunkFilters.length > 0 || conditionFilters.length > 0) {
            applyChunkedAndConditionFilters(values, chunkSize, ctx.resultsChunk, ctx.conditionFilterContexts);

            if (whereFilters.length == 0) {
                // fill the destination count chunk with the number of rows that passed the filter
                for (int dest = 0; dest < startPositions.size(); dest++) {
                    final int start = startPositions.get(dest);
                    final int len = length.get(dest);
                    destCountChunk.set(dest, countChunk(ctx.resultsChunk, start, len));
                }
                return;
            } else {
                // We need to build a row set for the next set of filters
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                for (int ii = 0; ii < chunkSize; ii++) {
                    if (ctx.resultsChunk.get(ii)) {
                        builder.appendKey(ii);
                    }
                }
                initialRows = builder.build();
            }
        } else {
            initialRows = RowSetFactory.flat(chunkSize);
        }

        if (chunkColumnSource != null) {
            // Clear the chunk sources and add the new sliced chunk
            chunkColumnSource.clear();
            chunkColumnSource.addChunk((WritableChunk<? extends Values>) values);
        }

        RowSet result = initialRows;
        for (WhereFilter whereFilter : whereFilters) {
            try (final RowSet ignored2 = result) {
                result = whereFilter.filter(result, RowSetFactory.flat(chunkSize), chunkSourceTable, false);
            }
        }
        try (final RowSet ignored = result;
                final RowSequence.Iterator it = result.getRowSequenceIterator()) {
            for (int ii = 0; ii < startPositions.size(); ii++) {
                final int startIndex = startPositions.get(ii);
                final int lastIndex = startIndex + length.get(ii);

                // Count how many rows passed the filter for this destination
                final int count = (int) it.advanceAndGetPositionDistance(lastIndex);
                destCountChunk.set(ii, count);
            }
        }
    }

    private int doCountSingleton(
            final CountWhereSingletonContext ctx,
            final Chunk<? extends Values> values,
            final int chunkSize) {

        final RowSet initialRows;
        if (chunkFilters.length > 0 || conditionFilters.length > 0) {
            final int count =
                    applyChunkedAndConditionFilters(values, chunkSize, ctx.resultsChunk, ctx.conditionFilterContexts);

            if (whereFilters.length == 0) {
                // No work to do, return the count of rows that passed the filters
                return count;
            } else {
                // We need to build a row set for the next set of filters
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                for (int ii = 0; ii < chunkSize; ii++) {
                    if (ctx.resultsChunk.get(ii)) {
                        builder.appendKey(ii);
                    }
                }
                initialRows = builder.build();
            }
        } else {
            initialRows = RowSetFactory.flat(chunkSize);
        }

        if (chunkColumnSource != null) {
            // Clear the chunk sources and add the new sliced chunk
            chunkColumnSource.clear();
            chunkColumnSource.addChunk((WritableChunk<? extends Values>) values);
        }

        // Apply the non-chunked filters against the row set
        RowSet result = initialRows;
        for (WhereFilter whereFilter : whereFilters) {
            try (final RowSet ignored2 = result) {
                result = whereFilter.filter(result, RowSetFactory.flat(chunkSize), chunkSourceTable, false);
            }
        }
        try (final RowSet ignored2 = result) {
            return result.intSize();
        }
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
        return new CountWhereBucketedContext(size, conditionFilters);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new CountWhereSingletonContext(size, conditionFilters);
    }
}
