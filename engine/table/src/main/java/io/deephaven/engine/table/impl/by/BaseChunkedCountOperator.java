//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;

abstract class BaseChunkedCountOperator implements IterativeChunkedAggregationOperator {
    private final String resultName;
    final LongArraySource resultColumnSource;

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    BaseChunkedCountOperator(@NotNull final String resultName) {
        this.resultName = resultName;
        this.resultColumnSource = new LongArraySource();
    }

    protected abstract int doCount(int chunkStart, int chunkSize, Chunk<? extends Values> values);

    @Override
    public void addChunk(
            BucketedContext context,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(values, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values, destination, 0, values.size());
    }

    private boolean addChunk(
            Chunk<? extends Values> values,
            long destination,
            int chunkStart,
            int chunkSize) {
        final int count = doCount(chunkStart, chunkSize, values);
        if (count > 0) {
            resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), count));
            return true;
        }
        return false;
    }

    @Override
    public void removeChunk(
            BucketedContext context,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(values, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean removeChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            long destination) {
        return removeChunk(values, destination, 0, values.size());
    }

    private boolean removeChunk(
            Chunk<? extends Values> values,
            long destination,
            int chunkStart,
            int chunkSize) {
        final int count = doCount(chunkStart, chunkSize, values);
        if (count > 0) {
            resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), -count));
            return true;
        }
        return false;
    }

    @Override
    public void modifyChunk(
            BucketedContext context,
            Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii,
                    modifyChunk(previousValues, newValues, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean modifyChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends RowKeys> postShiftRowKeys,
            long destination) {
        return modifyChunk(previousValues, newValues, destination, 0, newValues.size());
    }

    private boolean modifyChunk(
            Chunk<? extends Values> oldValues,
            Chunk<? extends Values> newValues,
            long destination,
            int chunkStart,
            int chunkSize) {
        final int oldCount = doCount(chunkStart, chunkSize, oldValues);
        final int newCount = doCount(chunkStart, chunkSize, newValues);
        final int count = newCount - oldCount;
        if (count != 0) {
            resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), count));
            return true;
        }
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumnSource.ensureCapacity(tableSize, false);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(resultName, resultColumnSource);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumnSource.startTrackingPrevValues();
    }

}
