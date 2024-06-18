//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.util.NullSafeAddition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.table.impl.by.RollupConstants.*;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

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
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final FloatChunk<? extends Values> asFloatChunk = values.asFloatChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asFloatChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final FloatChunk<? extends Values> asFloatChunk = values.asFloatChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asFloatChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asFloatChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asFloatChunk(), destination, 0, values.size());
    }

    private boolean addChunk(FloatChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableDouble sum2 = new MutableDouble();
        final MutableInt chunkNormalCount = new MutableInt();
        final MutableInt chunkNanCount = new MutableInt();
        final MutableInt chunkInfinityCount = new MutableInt();
        final MutableInt chunkMinusInfinity = new MutableInt();
        final double sum = SumFloatChunk.sum2FloatChunk(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount,
                chunkInfinityCount, chunkMinusInfinity, sum2);

        final long totalPositiveInfinities = updatePositiveInfinityCount(destination, chunkInfinityCount.get());
        final long totalNegativeInfinities = updateNegativeInfinityCount(destination, chunkMinusInfinity.get());
        final long totalNanCount = updateNanCount(destination, chunkNanCount.get());
        final boolean forceNanResult = totalNegativeInfinities > 0 || totalPositiveInfinities > 0 || totalNanCount > 0;

        if (chunkNormalCount.get() > 0) {
            final long totalNormalCount = nonNullCounter.addNonNullUnsafe(destination, chunkNormalCount.get());
            final double newSum = NullSafeAddition.plusDouble(sumSource.getUnsafe(destination), sum);
            final double newSum2 = NullSafeAddition.plusDouble(sum2Source.getUnsafe(destination), sum2.doubleValue());

            sumSource.set(destination, newSum);
            sum2Source.set(destination, newSum2);

            Assert.neqZero(totalNormalCount, "totalNormalCount");
            if (forceNanResult || totalNormalCount == 1) {
                resultColumn.set(destination, Double.NaN);
            } else {
                // If the sum or sumSquared has reached +/-Infinity, we are stuck with NaN forever.
                if (Double.isInfinite(newSum) || Double.isInfinite(newSum2)) {
                    resultColumn.set(destination, Double.NaN);
                    return true;
                }
                final double variance = computeVariance(totalNormalCount, newSum, newSum2);
                resultColumn.set(destination, std ? Math.sqrt(variance) : variance);
            }
            return true;
        }
        if (forceNanResult) {
            resultColumn.set(destination, Double.NaN);
            return true;
        }
        final long totalNormalCount = nonNullCounter.getCountUnsafe(destination);
        if (totalNormalCount == 0) {
            resultColumn.set(destination, NULL_DOUBLE);
            return true;
        } else if (totalNormalCount == 1) {
            resultColumn.set(destination, Double.NaN);
            return true;
        }
        return false;
    }

    private static double computeVariance(long nonNullCount, double newSum, double newSum2) {
        // Perform the calculation in a way that minimizes the impact of FP error.
        final double eps = Math.ulp(newSum2);
        final double vs2bar = newSum * (newSum / nonNullCount);
        final double delta = newSum2 - vs2bar;
        final double rel_eps = delta / eps;

        // Return zero when the variance is leq the FP error or when variance becomes negative
        final double variance = Math.abs(rel_eps) > 1.0 ? delta / (nonNullCount - 1) : 0.0;
        return Math.max(variance, 0.0);
    }

    private boolean removeChunk(FloatChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableDouble sum2 = new MutableDouble();
        final MutableInt chunkNormalCount = new MutableInt();
        final MutableInt chunkNanCount = new MutableInt();
        final MutableInt chunkInfinityCount = new MutableInt();
        final MutableInt chunkMinusInfinity = new MutableInt();
        final double sum = SumFloatChunk.sum2FloatChunk(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount,
                chunkInfinityCount, chunkMinusInfinity, sum2);

        if (chunkNormalCount.get() == 0 && chunkNanCount.get() == 0 && chunkInfinityCount.get() == 0
                && chunkMinusInfinity.get() == 0) {
            return false;
        }

        final long totalPositiveInfinities = updatePositiveInfinityCount(destination, -chunkInfinityCount.get());
        final long totalNegativeInfinities = updateNegativeInfinityCount(destination, -chunkMinusInfinity.get());
        final long totalNanCount = updateNanCount(destination, -chunkNanCount.get());
        final boolean forceNanResult = totalNegativeInfinities > 0 || totalPositiveInfinities > 0 || totalNanCount > 0;

        final long totalNormalCount = nonNullCounter.addNonNullUnsafe(destination, -chunkNormalCount.get());
        final double newSum, newSum2;

        if (chunkNormalCount.get() > 0) {
            if (totalNormalCount > 0) {
                newSum = NullSafeAddition.plusDouble(sumSource.getUnsafe(destination), -sum);
                newSum2 = NullSafeAddition.plusDouble(sum2Source.getUnsafe(destination), -sum2.doubleValue());
            } else {
                newSum = newSum2 = 0;
            }
            sumSource.set(destination, newSum);
            sum2Source.set(destination, newSum2);
        } else {
            newSum = sumSource.getUnsafe(destination);
            newSum2 = sum2Source.getUnsafe(destination);
        }

        if (totalNormalCount == 1 || forceNanResult) {
            resultColumn.set(destination, Double.NaN);
            return true;
        } else if (totalNormalCount == 0) {
            resultColumn.set(destination, NULL_DOUBLE);
            return true;
        }

        // If the sum has reach +/-Infinity, we are stuck with NaN forever.
        if (Double.isInfinite(newSum) || Double.isInfinite(newSum2)) {
            resultColumn.set(destination, Double.NaN);
            return true;
        }

        // Perform the calculation in a way that minimizes the impact of FP error.
        final double variance = computeVariance(totalNormalCount, newSum, newSum2);
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
            results.put(name + ROLLUP_RUNNING_SUM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, sumSource);
            results.put(name + ROLLUP_RUNNING_SUM2_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, sum2Source);
            results.put(name + ROLLUP_NONNULL_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, nonNullCounter.getColumnSource());
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
