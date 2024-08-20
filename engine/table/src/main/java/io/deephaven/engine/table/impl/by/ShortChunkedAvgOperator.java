//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkedAvgOperator and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.mutable.MutableInt;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.table.impl.by.RollupConstants.*;
import static io.deephaven.engine.util.NullSafeAddition.plusLong;
import static io.deephaven.engine.util.NullSafeAddition.minusLong;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * Iterative average operator.
 */
class ShortChunkedAvgOperator implements IterativeChunkedAggregationOperator {
    private final DoubleArraySource resultColumn;
    private final LongArraySource runningSum;
    private final NonNullCounter nonNullCount;
    private final String name;
    private final boolean exposeInternalColumns;

    ShortChunkedAvgOperator(String name, boolean exposeInternalColumns) {
        this.name = name;
        this.exposeInternalColumns = exposeInternalColumns;
        resultColumn = new DoubleArraySource();
        runningSum = new LongArraySource();
        nonNullCount = new NonNullCounter();
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ShortChunk<? extends Values> asShortChunk = values.asShortChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asShortChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ShortChunk<? extends Values> asShortChunk = values.asShortChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asShortChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asShortChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asShortChunk(), destination, 0, values.size());
    }

    private boolean addChunk(ShortChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final ShortChunk<? extends Values> asShortChunk = values.asShortChunk();
        final long chunkSum = SumShortChunk.sumShortChunk(asShortChunk, chunkStart, chunkSize, chunkNonNull);

        if (chunkNonNull.get() > 0) {
            final long newCount = nonNullCount.addNonNullUnsafe(destination, chunkNonNull.get());
            final long newSum = plusLong(runningSum.getUnsafe(destination), chunkSum);
            runningSum.set(destination, newSum);
            resultColumn.set(destination, (double) newSum / newCount);
        } else if (nonNullCount.onlyNullsUnsafe(destination)) {
            resultColumn.set(destination, NULL_DOUBLE);
        } else {
            return false;
        }
        return true;
    }

    private boolean removeChunk(ShortChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final long chunkSum = SumShortChunk.sumShortChunk(values, chunkStart, chunkSize, chunkNonNull);

        if (chunkNonNull.get() == 0) {
            return false;
        }

        final long newCount = nonNullCount.addNonNullUnsafe(destination, -chunkNonNull.get());
        final long newSum = minusLong(runningSum.getUnsafe(destination), chunkSum);
        runningSum.set(destination, newSum);
        if (newCount == 0) {
            resultColumn.set(destination, NULL_DOUBLE);
        } else {
            resultColumn.set(destination, (double) newSum / newCount);
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
