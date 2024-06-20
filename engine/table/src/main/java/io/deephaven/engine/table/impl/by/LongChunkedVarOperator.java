//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkedVarOperator and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
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
import static io.deephaven.engine.util.NullSafeAddition.plusDouble;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * Iterative variance operator.
 */
class LongChunkedVarOperator implements IterativeChunkedAggregationOperator {
    private final boolean std;
    private final String name;
    private final boolean exposeInternalColumns;
    private final NonNullCounter nonNullCounter = new NonNullCounter();
    private final DoubleArraySource resultColumn = new DoubleArraySource();
    private final DoubleArraySource sumSource = new DoubleArraySource();
    private final DoubleArraySource sum2Source = new DoubleArraySource();

    LongChunkedVarOperator(boolean std, String name, boolean exposeInternalColumns) {
        this.std = std;
        this.name = name;
        this.exposeInternalColumns = exposeInternalColumns;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final LongChunk<? extends Values> asLongChunk = values.asLongChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asLongChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final LongChunk<? extends Values> asLongChunk = values.asLongChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asLongChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asLongChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asLongChunk(), destination, 0, values.size());
    }

    private boolean addChunk(LongChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableDouble sum2 = new MutableDouble();
        final MutableInt chunkNonNull = new MutableInt();
        final double sum = SumLongChunk.sum2LongChunk(values, chunkStart, chunkSize, chunkNonNull, sum2);

        if (chunkNonNull.get() > 0) {
            final long totalNormalCount = nonNullCounter.addNonNullUnsafe(destination, chunkNonNull.get());
            final double newSum = plusDouble(sumSource.getUnsafe(destination), sum);
            final double newSum2 = plusDouble(sum2Source.getUnsafe(destination), sum2.doubleValue());

            sumSource.set(destination, newSum);
            sum2Source.set(destination, newSum2);

            Assert.neqZero(totalNormalCount, "totalNormalCount");
            if (totalNormalCount == 1) {
                resultColumn.set(destination, Double.NaN);
            } else {
                final double variance = (newSum2 - (newSum * newSum / totalNormalCount)) / (totalNormalCount - 1);
                resultColumn.set(destination, std ? Math.sqrt(variance) : variance);
            }
        } else {
            final long totalNormalCount = nonNullCounter.getCountUnsafe(destination);
            if (totalNormalCount == 0) {
                resultColumn.set(destination, NULL_DOUBLE);
            } else if (totalNormalCount == 1) {
                resultColumn.set(destination, Double.NaN);
            }
        }
        return true;
    }

    private boolean removeChunk(LongChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableDouble sum2 = new MutableDouble();
        final MutableInt chunkNonNull = new MutableInt();
        final double sum = SumLongChunk.sum2LongChunk(values, chunkStart, chunkSize, chunkNonNull, sum2);

        if (chunkNonNull.get() == 0) {
            return false;
        }

        final long totalNormalCount = nonNullCounter.addNonNullUnsafe(destination, -chunkNonNull.get());

        final double newSum;
        final double newSum2;

        if (totalNormalCount == 0) {
            newSum = newSum2 = 0;
        } else {
            newSum = plusDouble(sumSource.getUnsafe(destination), -sum);
            newSum2 = plusDouble(sum2Source.getUnsafe(destination), -sum2.doubleValue());
        }

        sumSource.set(destination, newSum);
        sum2Source.set(destination, newSum2);

        if (totalNormalCount == 0) {
            resultColumn.set(destination, NULL_DOUBLE);
            return true;
        }
        if (totalNormalCount == 1) {
            resultColumn.set(destination, Double.NaN);
            return true;
        }

        final double variance = (newSum2 - (newSum * newSum / totalNormalCount)) / (totalNormalCount - 1);
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
