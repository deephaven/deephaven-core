/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.by;

import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.DoubleArraySource;
import io.deephaven.engine.v2.sources.LongArraySource;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.*;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;
import static io.deephaven.engine.util.NullSafeAddition.minusLong;
import static io.deephaven.engine.v2.by.ComboAggregateFactory.*;

/**
 * Iterative average operator.
 */
class CharChunkedAvgOperator implements IterativeChunkedAggregationOperator {
    private final DoubleArraySource resultColumn;
    private final LongArraySource runningSum;
    private final NonNullCounter nonNullCount;
    private final String name;
    private final boolean exposeInternalColumns;

    CharChunkedAvgOperator(String name, boolean exposeInternalColumns) {
        this.name = name;
        this.exposeInternalColumns = exposeInternalColumns;
        resultColumn = new DoubleArraySource();
        runningSum = new LongArraySource();
        nonNullCount = new NonNullCounter();
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputIndices, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final CharChunk<? extends Values> asCharChunk = values.asCharChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asCharChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputIndices, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final CharChunk<? extends Values> asCharChunk = values.asCharChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asCharChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputIndices, long destination) {
        return addChunk(values.asCharChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputIndices, long destination) {
        return removeChunk(values.asCharChunk(), destination, 0, values.size());
    }
    
    private boolean addChunk(CharChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final CharChunk<? extends Values> asCharChunk = values.asCharChunk();
        final long chunkSum = SumCharChunk.sumCharChunk(asCharChunk, chunkStart, chunkSize, chunkNonNull);

        if (chunkNonNull.intValue() > 0) {
            final long newCount = nonNullCount.addNonNullUnsafe(destination, chunkNonNull.intValue());
            final long newSum = plusLong(runningSum.getUnsafe(destination), chunkSum);
            runningSum.set(destination, newSum);
            resultColumn.set(destination, (double)newSum / newCount);
        } else if (nonNullCount.onlyNullsUnsafe(destination)) {
            resultColumn.set(destination, Double.NaN);
        } else {
            return false;
        }
        return true;
    }

    private boolean removeChunk(CharChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final long chunkSum = SumCharChunk.sumCharChunk(values, chunkStart, chunkSize, chunkNonNull);

        if (chunkNonNull.intValue() == 0) {
            return false;
        }

        final long newCount = nonNullCount.addNonNullUnsafe(destination, -chunkNonNull.intValue());
        final long newSum = minusLong(runningSum.getUnsafe(destination), chunkSum);
        runningSum.set(destination, newSum);
        resultColumn.set(destination, (double)newSum / newCount);

        return true;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
        nonNullCount.ensureCapacity(tableSize);
        runningSum.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        if (exposeInternalColumns) {
            final Map<String, ColumnSource<?>> results = new LinkedHashMap<>();
            results.put(name, resultColumn);
            results.put(name + ROLLUP_RUNNING_SUM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, runningSum);
            results.put(name + ROLLUP_NONNULL_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, nonNullCount.getColumnSource());
            return results;
        } else {
            return Collections.singletonMap(name, resultColumn);
        }
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
        if (exposeInternalColumns) {
            runningSum.startTrackingPrevValues();
            nonNullCount.startTrackingPrevValues();
        }
    }
}
