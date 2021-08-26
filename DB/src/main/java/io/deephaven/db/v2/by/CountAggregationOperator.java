package io.deephaven.db.v2.by;

import io.deephaven.db.util.NullSafeAddition;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LongArraySource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;

import java.util.Collections;
import java.util.Map;

class CountAggregationOperator implements IterativeChunkedAggregationOperator {
    private final String resultName;
    private final LongArraySource countColumnSource;

    CountAggregationOperator(String resultName) {
        this.resultName = resultName;
        this.countColumnSource = new LongArraySource();
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final long newCount = length.get(ii);
            final long oldCount = countColumnSource.getUnsafe(destination);
            countColumnSource.set(destination, NullSafeAddition.plusLong(oldCount, newCount));
        }
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final long newCount = length.get(ii);
            final long oldCount = countColumnSource.getUnsafe(destination);
            countColumnSource.set(destination, NullSafeAddition.minusLong(oldCount, newCount));
        }
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        final long oldCount = countColumnSource.getUnsafe(destination);
        countColumnSource.set(destination, NullSafeAddition.plusLong(oldCount, chunkSize));
        return true;
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        final long oldCount = countColumnSource.getUnsafe(destination);
        countColumnSource.set(destination, NullSafeAddition.minusLong(oldCount, chunkSize));
        return true;
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        stateModified.fillWithValue(0, startPositions.size(), false);
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        countColumnSource.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(resultName, countColumnSource);
    }

    @Override
    public void startTrackingPrevValues() {
        countColumnSource.startTrackingPrevValues();
    }
}
