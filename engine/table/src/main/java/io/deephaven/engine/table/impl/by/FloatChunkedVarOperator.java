/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.util.NullSafeAddition;
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

/**
 * Iterative variance operator.
 */
final class FloatChunkedVarOperator extends FpChunkedNonNormalCounter implements IterativeChunkedAggregationOperator {
    private final String name;
    private final boolean exposeInternalColumns;
    private final boolean std;
    private final NonNullCounter nonNullCounter = new NonNullCounter();
    private final DoubleArraySource resultColumn = new DoubleArraySource();
    private final DoubleArraySource sumSource = new DoubleArraySource();
    private final DoubleArraySource sum2Source = new DoubleArraySource();

    FloatChunkedVarOperator(boolean std, String name, boolean exposeInternalColumns) {
        this.std = std;
        this.name = name;
        this.exposeInternalColumns = exposeInternalColumns;
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final FloatChunk<? extends Values> asFloatChunk = values.asFloatChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asFloatChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final FloatChunk<? extends Values> asFloatChunk = values.asFloatChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asFloatChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asFloatChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asFloatChunk(), destination, 0, values.size());
    }

    private boolean addChunk(FloatChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableDouble sum2 = new MutableDouble();
        final MutableInt chunkNormalCount = new MutableInt();
        final MutableInt chunkNanCount = new MutableInt();
        final MutableInt chunkInfinityCount = new MutableInt();
        final MutableInt chunkMinusInfinity = new MutableInt();
        final double sum = SumFloatChunk.sum2FloatChunk(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount, chunkInfinityCount, chunkMinusInfinity, sum2);

        final long totalPositiveInfinities = updatePositiveInfinityCount(destination, chunkInfinityCount.intValue());
        final long totalNegativeInfinities = updateNegativeInfinityCount(destination, chunkMinusInfinity.intValue());
        final long totalNanCount = updateNanCount(destination, chunkNanCount.intValue());
        final boolean forceNanResult = totalNegativeInfinities > 0 || totalPositiveInfinities > 0 || totalNanCount > 0;

        if (chunkNormalCount.intValue() > 0) {
            final long nonNullCount = nonNullCounter.addNonNullUnsafe(destination, chunkNormalCount.intValue());
            final double newSum = NullSafeAddition.plusDouble(sumSource.getUnsafe(destination), sum);
            final double newSum2 = NullSafeAddition.plusDouble(sum2Source.getUnsafe(destination), sum2.doubleValue());

            sumSource.set(destination, newSum);
            sum2Source.set(destination, newSum2);

            if (forceNanResult || nonNullCount <= 1) {
                resultColumn.set(destination, Double.NaN);
            } else {
                final double variance = (newSum2 - newSum * newSum / nonNullCount) / (nonNullCount - 1);
                resultColumn.set(destination, std ? Math.sqrt(variance) : variance);
            }
            return true;
        } else if (forceNanResult || (nonNullCounter.getCountUnsafe(destination) <= 1)) {
            resultColumn.set(destination, Double.NaN);
            return true;
        } else {
            return false;
        }
    }


    private boolean removeChunk(FloatChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableDouble sum2 = new MutableDouble();
        final MutableInt chunkNormalCount = new MutableInt();
        final MutableInt chunkNanCount = new MutableInt();
        final MutableInt chunkInfinityCount = new MutableInt();
        final MutableInt chunkMinusInfinity = new MutableInt();
        final double sum = SumFloatChunk.sum2FloatChunk(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount, chunkInfinityCount, chunkMinusInfinity, sum2);

        if (chunkNormalCount.intValue() == 0 && chunkNanCount.intValue() == 0 && chunkInfinityCount.intValue() == 0 && chunkMinusInfinity.intValue() == 0) {
            return false;
        }

        final long totalPositiveInfinities = updatePositiveInfinityCount(destination, -chunkInfinityCount.intValue());
        final long totalNegativeInfinities = updateNegativeInfinityCount(destination, -chunkMinusInfinity.intValue());
        final long totalNanCount = updateNanCount(destination, -chunkNanCount.intValue());
        final boolean forceNanResult = totalNegativeInfinities > 0 || totalPositiveInfinities > 0 || totalNanCount > 0;

        final long totalNormalCount = nonNullCounter.addNonNullUnsafe(destination, -chunkNormalCount.intValue());
        final double newSum, newSum2;

        if (chunkNormalCount.intValue() > 0) {
            if (totalNormalCount > 0) {
                newSum = NullSafeAddition.plusDouble(sumSource.getUnsafe(destination), -sum);
                newSum2 = NullSafeAddition.plusDouble(sum2Source.getUnsafe(destination), -sum2.doubleValue());
            } else {
                newSum = newSum2 = 0;
            }
            sumSource.set(destination, newSum);
            sum2Source.set(destination, newSum2);
        } else if (totalNormalCount <= 1 || forceNanResult) {
            resultColumn.set(destination, Double.NaN);
            return true;
        } else {
            newSum = sumSource.getUnsafe(destination);
            newSum2 = sum2Source.getUnsafe(destination);
        }
        if (totalNormalCount <= 1) {
            resultColumn.set(destination, Double.NaN);
            return true;
        }
        final double variance = (newSum2 - newSum * newSum / totalNormalCount) / (totalNormalCount - 1);
        resultColumn.set(destination, std ? Math.sqrt(variance) : variance);
        return true;
    }


    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
        nonNullCounter.ensureCapacity(tableSize);
        sumSource.ensureCapacity(tableSize);
        sum2Source.ensureCapacity(tableSize);
        ensureNonNormalCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        if (exposeInternalColumns) {
            final Map<String, ColumnSource<?>> results = new LinkedHashMap<>();
            results.put(name, resultColumn);
            results.put(name + ROLLUP_RUNNING_SUM_COLUMN_ID + ROLLUP_INTERNAL_COLUMN_SUFFIX, sumSource);
            results.put(name + ROLLUP_RUNNING_SUM2_COLUMN_ID + ROLLUP_INTERNAL_COLUMN_SUFFIX, sum2Source);
            results.put(name + ROLLUP_NONNULL_COUNT_COLUMN_ID + ROLLUP_INTERNAL_COLUMN_SUFFIX, nonNullCounter.getColumnSource());
            results.putAll(fpInternalColumnSources(name));
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
            startTrackingPrevFpCounterValues();
        }
    }
}
