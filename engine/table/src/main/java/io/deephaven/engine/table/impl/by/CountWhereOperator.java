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
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.ExposesChunkFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;
import static io.deephaven.engine.util.TableTools.emptyTable;

public class CountWhereOperator implements IterativeChunkedAggregationOperator {
    private final String resultName;
    private final LongArraySource resultColumnSource;
    private final WhereFilter[] whereFilters;
    private final ChunkFilter[] chunkFilters;


    private WritableBooleanChunk<Values> resultsChunk;
    private WritableIntChunk<Values> countChunk;
    private WritableIntChunk<Values> previousCountChunk;

    /**
     * A table constructed from chunk sources, will be populated with incoming chunk data and used to evaluate filters.
     */
    final Table chunkSourceTable;
    final ChunkColumnSource<?> chunkColumnSource;

    // static class CountWhereBucketedContext implements BucketedContext {
    // final WritableBooleanChunk<Values> resultsChunk;
    // final WritableIntChunk<Values> countChunk;
    // // extra chunk for comparing previous values
    // final WritableIntChunk<Values> previousCountChunk;
    //
    // public CountWhereBucketedContext(final int size, final ChunkType chunkType) {
    // resultsChunk = WritableBooleanChunk.makeWritableChunk(size);
    // countChunk = WritableIntChunk.makeWritableChunk(size);
    // previousCountChunk = WritableIntChunk.makeWritableChunk(size);
    // }
    //
    // @Override
    // public void close() {
    // resultsChunk.close();
    // countChunk.close();
    // previousCountChunk.close();
    // }
    // }
    //
    // static class CountWhereSingletonContext implements SingletonContext {
    // final WritableBooleanChunk<Values> resultsChunk;
    //
    // public CountWhereSingletonContext(final int size, final ChunkType chunkType) {
    // resultsChunk = WritableBooleanChunk.makeWritableChunk(size);
    // }
    //
    // @Override
    // public void close() {
    // resultsChunk.close();
    // }
    // }

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    CountWhereOperator(
            @NotNull final String resultName,
            final WhereFilter[] whereFilters,
            @Nullable final String inputColumnName,
            @Nullable final ColumnSource<?> inputSource) {
        this.resultName = resultName;
        this.resultColumnSource = new LongArraySource();

        final Map<Boolean, List<WhereFilter>> partitioned = Arrays.stream(whereFilters)
                .collect(Collectors.partitioningBy(filter -> filter instanceof ExposesChunkFilter
                        && ((ExposesChunkFilter) filter).chunkFilter().isPresent()));

        this.whereFilters = partitioned.get(false).toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

        chunkFilters = partitioned.get(true).stream()
                .map(filter -> ((ExposesChunkFilter) filter).chunkFilter().get())
                .toArray(ChunkFilter[]::new);

        if (inputColumnName == null || inputSource == null) {
            chunkColumnSource = null;
            chunkSourceTable = emptyTable(0);
        } else {
            chunkColumnSource = ChunkColumnSource.make(inputSource.getChunkType(), inputSource.getType(),
                    inputSource.getComponentType());
            chunkSourceTable = new QueryTable(RowSetFactory.empty().toTracking(),
                    Collections.singletonMap(inputColumnName, chunkColumnSource));
        }
    }

    /**
     * Given the data chunk and multiple destinations, count the number of rows that pass the filters and store into the
     * count chunk.
     */
    private void doCountBucketed(
            final Chunk<? extends Values> values,
            final IntChunk<RowKeys> destinations,
            final IntChunk<ChunkPositions> startPositions,
            final IntChunk<ChunkLengths> length,
            final WritableBooleanChunk<Values> resultsChunk,
            final WritableIntChunk<Values> countChunk) {
        final RowSet initialRows;
        // values can be null, so get the length from the positions and length chunks
        final int chunkSize = startPositions.get(startPositions.size() - 1) + length.get(length.size() - 1);

        if (chunkFilters.length > 0) {
            // Pre-fill fill TRUE since the filtering will only set FALSE
            resultsChunk.fillWithValue(0, chunkSize, true);

            for (int ii = 0; ii < chunkFilters.length; ii++) {
                chunkFilters[ii].filter(values, resultsChunk);
            }

            if (whereFilters.length == 0) {
                // fill the destination count chunk with the number of rows that passed the filter
                for (int dest = 0; dest < startPositions.size(); dest++) {
                    final int start = startPositions.get(dest);
                    final int len = length.get(dest);
                    countChunk.set(dest, countChunk(resultsChunk, start, len));
                }
                return;
            } else {
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                for (int ii = 0; ii < chunkSize; ii++) {
                    if (resultsChunk.get(ii)) {
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
                countChunk.set(ii, count);
            }
        }
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

    private int doCountSingleton(
            final Chunk<? extends Values> values,
            final int chunkSize,
            final WritableBooleanChunk<Values> resultsChunk) {

        final RowSet initialRows;

        if (chunkFilters.length > 0) {
            // Pre-fill fill TRUE since the filtering will only set FALSE
            resultsChunk.fillWithValue(0, chunkSize, true);

            int count = 0;
            // Apply the filters and keep a count of the number of rows that fail
            for (int ii = 0; ii < chunkFilters.length; ii++) {
                final ChunkFilter filter = chunkFilters[ii];
                count += filter.filter(values, resultsChunk);
            }

            if (whereFilters.length == 0) {
                return chunkSize - count;
            } else {
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                for (int ii = 0; ii < chunkSize; ii++) {
                    if (resultsChunk.get(ii)) {
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

        final int chunkSize = startPositions.get(startPositions.size() - 1) + length.get(length.size() - 1);
        ensureWorkingChunks(chunkSize);

        // Do the counting work and store the results in the count chunk
        doCountBucketed(values, destinations, startPositions, length, resultsChunk, countChunk);

        // Update the result column source with the counts
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int count = countChunk.get(ii);
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
        ensureWorkingChunks(chunkSize);

        final int count = doCountSingleton(values, chunkSize, resultsChunk);
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
        // Do the counting work and store the results in the count chunk
        doCountBucketed(values, destinations, startPositions, length, resultsChunk, countChunk);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int count = countChunk.get(ii);
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
        final int count = doCountSingleton(values, chunkSize, resultsChunk);
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
        // Do the counting work and store the results in the count chunk
        doCountBucketed(previousValues, destinations, startPositions, length, resultsChunk, previousCountChunk);
        doCountBucketed(newValues, destinations, startPositions, length, resultsChunk, countChunk);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int oldCount = previousCountChunk.get(ii);
            final int newCount = countChunk.get(ii);
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
        final int oldCount = doCountSingleton(previousValues, chunkSize, resultsChunk);
        final int newCount = doCountSingleton(newValues, chunkSize, resultsChunk);
        final int count = newCount - oldCount;
        if (count != 0) {
            final long updatedCount = plusLong(resultColumnSource.getUnsafe(destination), count);
            Assert.geqZero(updatedCount, "updatedCount");
            resultColumnSource.set(destination, updatedCount);
            return true;
        }
        return false;
    }

    /**
     * Ensure that the working chunks are large enough to hold the given number of values.
     */
    private void ensureWorkingChunks(int chunkSize) {
        if (resultsChunk != null && resultsChunk.size() < chunkSize) {
            resultsChunk.close();
            resultsChunk = null;
        }
        if (resultsChunk == null) {
            resultsChunk = WritableBooleanChunk.makeWritableChunk(chunkSize);
        }

        if (countChunk != null && countChunk.size() < chunkSize) {
            countChunk.close();
            countChunk = null;
        }
        if (countChunk == null) {
            countChunk = WritableIntChunk.makeWritableChunk(chunkSize);
        }

        if (previousCountChunk != null && previousCountChunk.size() < chunkSize) {
            previousCountChunk.close();
            previousCountChunk = null;
        }
        if (previousCountChunk == null) {
            previousCountChunk = WritableIntChunk.makeWritableChunk(chunkSize);
        }
    }

    /**
     * Release the working chunks.
     */
    private void releaseWorkingChunks() {
        if (resultsChunk != null) {
            resultsChunk.close();
            resultsChunk = null;
        }
        if (countChunk != null) {
            countChunk.close();
            countChunk = null;
        }
        if (previousCountChunk != null) {
            previousCountChunk.close();
            previousCountChunk = null;
        }
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable, int startingDestinationsCount) {
        IterativeChunkedAggregationOperator.super.propagateInitialState(resultTable, startingDestinationsCount);
        releaseWorkingChunks();
    }

    @Override
    public void propagateUpdates(@NotNull TableUpdate downstream, @NotNull RowSet newDestinations) {
        IterativeChunkedAggregationOperator.super.propagateUpdates(downstream, newDestinations);
        releaseWorkingChunks();
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

    // @Override
    // public BucketedContext makeBucketedContext(int size) {
    // return new CountWhereBucketedContext(size, chunkColumnSource == null ? null : chunkColumnSource.getChunkType());
    // }
    //
    // @Override
    // public SingletonContext makeSingletonContext(int size) {
    // return new CountWhereSingletonContext(size,
    // chunkColumnSource == null ? null : chunkColumnSource.getChunkType());
    // }
}
