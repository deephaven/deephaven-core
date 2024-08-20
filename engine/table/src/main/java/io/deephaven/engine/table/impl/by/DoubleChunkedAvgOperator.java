//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatChunkedAvgOperator and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.mutable.MutableInt;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.table.impl.by.RollupConstants.*;
import static io.deephaven.engine.util.NullSafeAddition.plusDouble;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

class DoubleChunkedAvgOperator extends FpChunkedNonNormalCounter implements IterativeChunkedAggregationOperator {
    private final String name;
    private final boolean exposeInternalColumns;
    private final DoubleArraySource resultColumn = new DoubleArraySource();
    private final DoubleArraySource runningSum = new DoubleArraySource();
    private final NonNullCounter nonNullCounter = new NonNullCounter();

    DoubleChunkedAvgOperator(String name, boolean exposeInternalColumns) {
        this.name = name;
        this.exposeInternalColumns = exposeInternalColumns;
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final DoubleChunk<? extends Values> asDoubleChunk = values.asDoubleChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asDoubleChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final DoubleChunk<? extends Values> asDoubleChunk = values.asDoubleChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asDoubleChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asDoubleChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asDoubleChunk(), destination, 0, values.size());
    }

    private boolean addChunk(DoubleChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNormalCount = new MutableInt(0);
        final MutableInt chunkNanCount = new MutableInt(0);
        final MutableInt chunkInfinityCount = new MutableInt(0);
        final MutableInt chunkMinusInfinityCount = new MutableInt(0);

        final double sum = SumDoubleChunk.sumDoubleChunk(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount,
                chunkInfinityCount, chunkMinusInfinityCount);

        final long totalNormal = nonNullCounter.addNonNullUnsafe(destination, chunkNormalCount.get());
        final long totalNanCount = updateNanCount(destination, chunkNanCount.get());
        final long totalPositiveInfinityCount = updatePositiveInfinityCount(destination, chunkInfinityCount.get());
        final long totalNegativeInfinityCount =
                updateNegativeInfinityCount(destination, chunkMinusInfinityCount.get());

        if (chunkNormalCount.get() > 0) {
            final double newSum = plusDouble(runningSum.getUnsafe(destination), sum);
            runningSum.set(destination, newSum);
            updateResultWithNewSum(destination, totalNormal, totalNanCount, totalPositiveInfinityCount,
                    totalNegativeInfinityCount, newSum);
        } else {
            updateResultSumUnchanged(destination, totalNormal, totalNanCount, totalPositiveInfinityCount,
                    totalNegativeInfinityCount);
        }
        return true;
    }

    private boolean removeChunk(DoubleChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNormalCount = new MutableInt(0);
        final MutableInt chunkNanCount = new MutableInt(0);
        final MutableInt chunkInfinityCount = new MutableInt(0);
        final MutableInt chunkMinusInfinityCount = new MutableInt(0);

        final double sum = SumDoubleChunk.sumDoubleChunk(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount,
                chunkInfinityCount, chunkMinusInfinityCount);
        if (chunkNormalCount.get() == 0 && chunkNanCount.get() == 0 && chunkInfinityCount.get() == 0
                && chunkMinusInfinityCount.get() == 0) {
            return false;
        }

        final long totalNormal = nonNullCounter.addNonNull(destination, -chunkNormalCount.get());
        final long totalNanCount = updateNanCount(destination, -chunkNanCount.get());
        final long totalPositiveInfinityCount =
                updatePositiveInfinityCount(destination, -chunkInfinityCount.get());
        final long totalNegativeInfinityCount =
                updateNegativeInfinityCount(destination, -chunkMinusInfinityCount.get());

        final double newSum;
        if (chunkNormalCount.get() > 0) {
            newSum = plusDouble(runningSum.getUnsafe(destination), -sum);
            runningSum.set(destination, newSum);
            updateResultWithNewSum(destination, totalNormal, totalNanCount, totalPositiveInfinityCount,
                    totalNegativeInfinityCount, newSum);
        } else {
            updateResultSumUnchanged(destination, totalNormal, totalNanCount, totalPositiveInfinityCount,
                    totalNegativeInfinityCount);
        }
        return true;
    }

    private void updateResultWithNewSum(long destination, long totalNormal, long totalNanCount, long totalInfinityCount,
            long totalNegativeInfinityCount, double newSum) {
        if (totalNanCount > 0 || (totalInfinityCount > 0 && totalNegativeInfinityCount > 0)) {
            resultColumn.set(destination, Double.NaN);
        } else if (totalInfinityCount > 0) {
            resultColumn.set(destination, Double.POSITIVE_INFINITY);
        } else if (totalNegativeInfinityCount > 0) {
            resultColumn.set(destination, Double.NEGATIVE_INFINITY);
        } else if (totalNormal == 0) {
            resultColumn.set(destination, NULL_DOUBLE);
        } else {
            resultColumn.set(destination, newSum / totalNormal);
        }
    }

    private void updateResultSumUnchanged(long destination, long totalNormal, long totalNanCount,
            long totalInfinityCount, long totalNegativeInfinityCount) {
        if (totalNanCount > 0 || (totalInfinityCount > 0 && totalNegativeInfinityCount > 0)) {
            resultColumn.set(destination, Double.NaN);
        } else if (totalInfinityCount > 0) {
            resultColumn.set(destination, Double.POSITIVE_INFINITY);
        } else if (totalNegativeInfinityCount > 0) {
            resultColumn.set(destination, Double.NEGATIVE_INFINITY);
        } else if (totalNormal == 0) {
            resultColumn.set(destination, NULL_DOUBLE);
        } else {
            resultColumn.set(destination, runningSum.getUnsafe(destination) / totalNormal);
        }
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
        nonNullCounter.ensureCapacity(tableSize);
        runningSum.ensureCapacity(tableSize);
        ensureNonNormalCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        if (exposeInternalColumns) {
            final Map<String, ColumnSource<?>> results = new LinkedHashMap<>();
            results.put(name, resultColumn);
            results.put(name + ROLLUP_RUNNING_SUM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, runningSum);
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
            runningSum.startTrackingPrevValues();
            nonNullCounter.startTrackingPrevValues();
            startTrackingPrevFpCounterValues();
        }
    }
}
