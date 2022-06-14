/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.BigDecimalUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.table.impl.by.RollupConstants.*;

/**
 * Iterative variance operator.
 */
class BigDecimalChunkedVarOperator implements IterativeChunkedAggregationOperator {
    private final static int SCALE =
            Configuration.getInstance().getIntegerWithDefault("BigDecimalStdOperator.scale", 10);

    private final String name;
    private final boolean exposeInternalColumns;
    private final boolean std;
    private final NonNullCounter nonNullCounter = new NonNullCounter();
    private final ObjectArraySource<BigDecimal> resultColumn = new ObjectArraySource<>(BigDecimal.class);
    private final ObjectArraySource<BigDecimal> sumSource = new ObjectArraySource<>(BigDecimal.class);
    private final ObjectArraySource<BigDecimal> sum2Source = new ObjectArraySource<>(BigDecimal.class);

    BigDecimalChunkedVarOperator(boolean std, String name, boolean exposeInternalColumns) {
        this.std = std;
        this.name = name;
        this.exposeInternalColumns = exposeInternalColumns;
    }

    private BigDecimal plus(BigDecimal a, BigDecimal b) {
        if (a == null) {
            if (b == null) {
                return BigDecimal.ZERO;
            }
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.add(b);
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigDecimal, ? extends Values> asObjectChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigDecimal, ? extends Values> asObjectChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    private boolean addChunk(ObjectChunk<BigDecimal, ? extends Values> values, long destination, int chunkStart,
            int chunkSize) {
        final MutableObject<BigDecimal> sum2 = new MutableObject<>();
        final MutableInt chunkNonNull = new MutableInt(0);
        final BigDecimal sum =
                SumBigDecimalChunk.sum2BigDecimalChunk(values, chunkStart, chunkSize, chunkNonNull, sum2);

        if (chunkNonNull.intValue() <= 0) {
            return false;
        }

        final BigDecimal newSum = plus(sumSource.getUnsafe(destination), sum);
        final BigDecimal newSum2 = plus(sum2Source.getUnsafe(destination), sum2.getValue());
        final long nonNullCount = nonNullCounter.addNonNullUnsafe(destination, chunkNonNull.intValue());
        doUpdate(destination, newSum, newSum2, nonNullCount);
        return true;
    }

    private boolean removeChunk(ObjectChunk<BigDecimal, ? extends Values> values, long destination, int chunkStart,
            int chunkSize) {
        final MutableObject<BigDecimal> sum2 = new MutableObject<>();
        final MutableInt chunkNonNull = new MutableInt(0);
        final BigDecimal sum =
                SumBigDecimalChunk.sum2BigDecimalChunk(values, chunkStart, chunkSize, chunkNonNull, sum2);

        if (chunkNonNull.intValue() <= 0) {
            return false;
        }

        final BigDecimal newSum = plus(sumSource.getUnsafe(destination), sum.negate());
        final BigDecimal newSum2 = plus(sum2Source.getUnsafe(destination), sum2.getValue().negate());
        final long nonNullCount = nonNullCounter.addNonNull(destination, -chunkNonNull.intValue());
        doUpdate(destination, newSum, newSum2, nonNullCount);
        return true;
    }

    private void doUpdate(long destination, BigDecimal newSum, BigDecimal newSum2, long nonNullCount) {
        if (nonNullCount == 0) {
            sumSource.set(destination, null);
            sum2Source.set(destination, null);
        } else {
            sumSource.set(destination, newSum);
            sum2Source.set(destination, newSum2);
        }

        if (nonNullCount <= 1) {
            resultColumn.set(destination, null);
        } else {
            final BigDecimal countMinus1 = BigDecimal.valueOf(nonNullCount - 1);
            final BigDecimal variance =
                    newSum2.subtract(newSum.pow(2).divide(BigDecimal.valueOf(nonNullCount), BigDecimal.ROUND_HALF_UP))
                            .divide(countMinus1, BigDecimal.ROUND_HALF_UP);
            if (std) {
                resultColumn.set(destination, BigDecimalUtils.sqrt(variance, SCALE));
            } else {
                resultColumn.set(destination, variance);
            }
        }
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
