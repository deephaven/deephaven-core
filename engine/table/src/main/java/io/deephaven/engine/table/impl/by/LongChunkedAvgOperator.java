/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkedAvgOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.table.impl.by.RollupConstants.*;
import static io.deephaven.engine.util.NullSafeAddition.plusLong;
import static io.deephaven.engine.util.NullSafeAddition.minusLong;

/**
 * Iterative average operator.
 */
class LongChunkedAvgOperator implements IterativeChunkedAggregationOperator {
    private final DoubleArraySource resultColumn;
    private final LongArraySource runningSum;
    private final NonNullCounter nonNullCount;
    private final String name;
    private final boolean exposeInternalColumns;

    LongChunkedAvgOperator(String name, boolean exposeInternalColumns) {
        this.name = name;
        this.exposeInternalColumns = exposeInternalColumns;
        resultColumn = new DoubleArraySource();
        runningSum = new LongArraySource();
        nonNullCount = new NonNullCounter();
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final LongChunk<? extends Values> asLongChunk = values.asLongChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asLongChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final LongChunk<? extends Values> asLongChunk = values.asLongChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asLongChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asLongChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asLongChunk(), destination, 0, values.size());
    }
    
    private boolean addChunk(LongChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final LongChunk<? extends Values> asLongChunk = values.asLongChunk();
        final long chunkSum = SumLongChunk.sumLongChunk(asLongChunk, chunkStart, chunkSize, chunkNonNull);

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

    private boolean removeChunk(LongChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final long chunkSum = SumLongChunk.sumLongChunk(values, chunkStart, chunkSize, chunkNonNull);

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
