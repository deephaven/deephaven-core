//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkedSumOperator and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;

/**
 * Iterative sum operator, extended for absolute values and regular sums.
 */
public class LongChunkedSumOperator implements IterativeChunkedAggregationOperator, ChunkSource<Values> {
    private final boolean absolute;
    private final String name;
    private final LongArraySource resultColumn = new LongArraySource();
    private final NonNullCounter nonNullCount = new NonNullCounter();

    LongChunkedSumOperator(boolean absolute, String name) {
        this.absolute = absolute;
        this.name = name;
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
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
        final MutableInt nonNullCount = new MutableInt(0);
        final long sum = doSum(chunkStart, chunkSize, nonNullCount, values);
        return updateInternal(destination, sum, nonNullCount.intValue());
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final LongChunk<? extends Values> newAsLongChunk = newValues.asLongChunk();
        final LongChunk<? extends Values> oldAsLongChunk = previousValues.asLongChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii,
                    modifyChunk(oldAsLongChunk, newAsLongChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        return modifyChunk(previousValues.asLongChunk(), newValues.asLongChunk(), destination, 0, newValues.size());
    }

    private boolean modifyChunk(LongChunk<? extends Values> oldValues, LongChunk<? extends Values> newValues,
            long destination, int chunkStart, int chunkSize) {
        final MutableInt oldNonNullCount = new MutableInt(0);
        final MutableInt newNonNullCount = new MutableInt(0);
        final long oldSum = doSum(chunkStart, chunkSize, oldNonNullCount, oldValues);
        final long newSum = doSum(chunkStart, chunkSize, newNonNullCount, newValues);
        return updateInternal(destination, newSum - oldSum, oldNonNullCount.intValue(), newNonNullCount.intValue());
    }

    private boolean removeChunk(LongChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt nonNullCount = new MutableInt(0);
        final long sum = doSum(chunkStart, chunkSize, nonNullCount, values);
        return updateInternal(destination, -sum, -nonNullCount.intValue());
    }

    private long doSum(int chunkStart, int chunkSize, MutableInt nonNullCount,
            LongChunk<? extends Values> asLongChunk) {
        if (absolute) {
            return SumLongChunk.sumLongChunkAbs(asLongChunk, chunkStart, chunkSize, nonNullCount);
        } else {
            return SumLongChunk.sumLongChunk(asLongChunk, chunkStart, chunkSize, nonNullCount);
        }
    }

    private boolean updateInternal(long destPos, long sum, int nonNullValues) {
        if (nonNullValues > 0) {
            nonNullCount.addNonNullUnsafe(destPos, nonNullValues);
            resultColumn.set(destPos, plusLong(resultColumn.getUnsafe(destPos), sum));
            return true;
        } else if (nonNullValues < 0) {
            final long newCount = nonNullCount.addNonNullUnsafe(destPos, nonNullValues);
            if (newCount > 0) {
                resultColumn.set(destPos, plusLong(resultColumn.getUnsafe(destPos), sum));
            } else {
                resultColumn.set(destPos, QueryConstants.NULL_LONG);
            }
            return true;
        }
        return false;
    }

    private boolean updateInternal(long destPos, long difference, int oldNonNullValues, int newNonNullValues) {
        if (oldNonNullValues == 0 && newNonNullValues == 0) {
            return false;
        }
        final int nonNullDifference = newNonNullValues - oldNonNullValues;

        if (nonNullDifference == 0) {
            // we do not need to update, and we must be positive, so we can just change the sum
            if (difference == 0) {
                return false;
            }
            resultColumn.set(destPos, plusLong(resultColumn.getUnsafe(destPos), difference));
            return true;
        }

        final long newCount = nonNullCount.addNonNullUnsafe(destPos, nonNullDifference);
        if (newCount > 0) {
            resultColumn.set(destPos, plusLong(resultColumn.getUnsafe(destPos), difference));
        } else {
            resultColumn.set(destPos, QueryConstants.NULL_LONG);
        }
        return true;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
        nonNullCount.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(name, resultColumn);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }

    final long getResult(long destination) {
        return resultColumn.getLong(destination);
    }

    @Override
    public ChunkType getChunkType() {
        return resultColumn.getChunkType();
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return resultColumn.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return resultColumn.getChunk(context, firstKey, lastKey);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        resultColumn.fillChunk(context, destination, rowSequence);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return resultColumn.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return resultColumn.makeGetContext(chunkCapacity, sharedContext);
    }
}
