/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import org.apache.commons.lang3.mutable.MutableInt;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.db.v2.by.ComboAggregateFactory.*;

class BigDecimalChunkedAvgOperator implements IterativeChunkedAggregationOperator {
    private final String name;
    private final boolean exposeInternalColumns;
    private final ObjectArraySource<BigDecimal> resultColumn = new ObjectArraySource<>(BigDecimal.class);
    private final ObjectArraySource<BigDecimal> runningSum = new ObjectArraySource<>(BigDecimal.class);
    private final NonNullCounter nonNullCount = new NonNullCounter();

    BigDecimalChunkedAvgOperator(String name, boolean exposeInternalColumns) {
        this.name = name;
        this.exposeInternalColumns = exposeInternalColumns;
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
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
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
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
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return addChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return removeChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    public boolean addChunk(ObjectChunk<BigDecimal, ? extends Values> values, long destination, int chunkStart,
            int chunkSize) {
        final MutableInt chunkNonNullCount = new MutableInt();
        final BigDecimal chunkSum =
                SumBigDecimalChunk.sumBigDecimalChunk(values, chunkStart, chunkSize, chunkNonNullCount);

        if (chunkNonNullCount.intValue() <= 0) {
            return false;
        }

        final long newCount = nonNullCount.addNonNullUnsafe(destination, chunkNonNullCount.intValue());
        final BigDecimal newSum;
        final BigDecimal oldSum = runningSum.getUnsafe(destination);
        if (oldSum == null) {
            newSum = chunkSum;
        } else {
            newSum = oldSum.add(chunkSum);
        }
        runningSum.set(destination, newSum);
        resultColumn.set(destination, newSum.divide(BigDecimal.valueOf(newCount), BigDecimal.ROUND_HALF_UP));

        return true;
    }

    public boolean removeChunk(ObjectChunk<BigDecimal, ? extends Values> values, long destination, int chunkStart,
            int chunkSize) {
        final MutableInt chunkNonNullCount = new MutableInt();
        final BigDecimal chunkSum =
                SumBigDecimalChunk.sumBigDecimalChunk(values, chunkStart, chunkSize, chunkNonNullCount);

        if (chunkNonNullCount.intValue() <= 0) {
            return false;
        }

        final long newCount = nonNullCount.addNonNullUnsafe(destination, -chunkNonNullCount.intValue());
        if (newCount == 0) {
            resultColumn.set(destination, null);
            runningSum.set(destination, null);
        } else {
            final BigDecimal oldSum = runningSum.getUnsafe(destination);
            final BigDecimal newSum = oldSum.subtract(chunkSum);
            runningSum.set(destination, newSum);
            resultColumn.set(destination, newSum.divide(BigDecimal.valueOf(newCount), BigDecimal.ROUND_HALF_UP));
        }

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
