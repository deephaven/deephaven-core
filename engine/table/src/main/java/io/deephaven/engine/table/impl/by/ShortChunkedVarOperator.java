/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkedVarOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.table.impl.by.RollupConstants.*;
import static io.deephaven.engine.util.NullSafeAddition.plusDouble;

/**
 * Iterative variance operator.
 */
class ShortChunkedVarOperator implements IterativeChunkedAggregationOperator {
    private final boolean std;
    private final String name;
    private final boolean exposeInternalColumns;
    private final NonNullCounter nonNullCounter = new NonNullCounter();
    private final DoubleArraySource resultColumn = new DoubleArraySource();
    private final DoubleArraySource sumSource = new DoubleArraySource();
    private final DoubleArraySource sum2Source = new DoubleArraySource();

    ShortChunkedVarOperator(boolean std, String name, boolean exposeInternalColumns) {
        this.std = std;
        this.name = name;
        this.exposeInternalColumns = exposeInternalColumns;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ShortChunk<? extends Values> asShortChunk = values.asShortChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asShortChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ShortChunk<? extends Values> asShortChunk = values.asShortChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asShortChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asShortChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asShortChunk(), destination, 0, values.size());
    }

    private boolean addChunk(ShortChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableDouble sum2 = new MutableDouble();
        final MutableInt chunkNonNull = new MutableInt();
        final double sum = SumShortChunk.sum2ShortChunk(values, chunkStart, chunkSize, chunkNonNull, sum2);

        if (chunkNonNull.intValue() > 0) {
            final long nonNullCount = nonNullCounter.addNonNullUnsafe(destination, chunkNonNull.intValue());
            final double newSum = plusDouble(sumSource.getUnsafe(destination), sum);
            final double newSum2 = plusDouble(sum2Source.getUnsafe(destination), sum2.doubleValue());

            sumSource.set(destination, newSum);
            sum2Source.set(destination, newSum2);

            if (nonNullCount <= 1) {
                resultColumn.set(destination, Double.NaN);
            } else {
                final double variance = (newSum2 - (newSum * newSum / nonNullCount)) / (nonNullCount - 1);
                resultColumn.set(destination, std ? Math.sqrt(variance) : variance);
            }
        } else if (nonNullCounter.getCountUnsafe(destination) <= 1) {
            resultColumn.set(destination, Double.NaN);
        }
        return true;
    }

    private boolean removeChunk(ShortChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableDouble sum2 = new MutableDouble();
        final MutableInt chunkNonNull = new MutableInt();
        final double sum = SumShortChunk.sum2ShortChunk(values, chunkStart, chunkSize, chunkNonNull, sum2);

        if (chunkNonNull.intValue() == 0) {
            return false;
        }

        final long nonNullCount = nonNullCounter.addNonNullUnsafe(destination, -chunkNonNull.intValue());

        final double newSum;
        final double newSum2;

        if (nonNullCount == 0) {
            newSum = newSum2 = 0;
        } else {
            newSum = plusDouble(sumSource.getUnsafe(destination), -sum);
            newSum2 = plusDouble(sum2Source.getUnsafe(destination), -sum2.doubleValue());
        }

        sumSource.set(destination, newSum);
        sum2Source.set(destination, newSum2);

        if (nonNullCount <= 1) {
            resultColumn.set(destination, Double.NaN);
            return true;
        }

        final double variance = (newSum2 - (newSum * newSum / nonNullCount)) / (nonNullCount - 1);
        resultColumn.set(destination, std ? Math.sqrt(variance) : variance);

        return true;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
        nonNullCounter.ensureCapacity(tableSize);
        sumSource.ensureCapacity(tableSize);
        sum2Source.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        if (exposeInternalColumns) {
            final Map<String, ColumnSource<?>> results = new LinkedHashMap<>();
            results.put(name, resultColumn);
            results.put(name + ROLLUP_RUNNING_SUM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, sumSource);
            results.put(name + ROLLUP_RUNNING_SUM2_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, sum2Source);
            results.put(name + ROLLUP_NONNULL_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, nonNullCounter.getColumnSource());
            return results;
        } else {
            return Collections.singletonMap(name, resultColumn);
        }
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
        if (exposeInternalColumns) {
            sumSource.startTrackingPrevValues();
            sum2Source.startTrackingPrevValues();
            nonNullCounter.startTrackingPrevValues();
        }
    }
}
