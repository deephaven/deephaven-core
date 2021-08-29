package io.deephaven.engine.v2.by;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.select.MatchPair;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.LongArraySource;
import io.deephaven.engine.v2.sources.ReadOnlyRedirectedColumnSource;
import io.deephaven.engine.structures.chunk.Attributes.ChunkLengths;
import io.deephaven.engine.structures.chunk.Attributes.ChunkPositions;
import io.deephaven.engine.structures.chunk.Attributes.KeyIndices;
import io.deephaven.engine.structures.chunk.Attributes.Values;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.IntChunk;
import io.deephaven.engine.structures.chunk.LongChunk;
import io.deephaven.engine.structures.chunk.WritableBooleanChunk;
import io.deephaven.engine.v2.utils.LongColumnSourceRedirectionIndex;

import java.util.LinkedHashMap;
import java.util.Map;

abstract class BaseAddOnlyFirstOrLastChunkedOperator implements IterativeChunkedAggregationOperator {
    final boolean isFirst;
    final LongArraySource redirections;
    private final LongColumnSourceRedirectionIndex redirectionIndex;
    private final Map<String, ColumnSource<?>> resultColumns;

    BaseAddOnlyFirstOrLastChunkedOperator(boolean isFirst, MatchPair[] resultPairs, Table originalTable,
            String exposeRedirectionAs) {
        this.isFirst = isFirst;
        this.redirections = new LongArraySource();
        this.redirectionIndex = new LongColumnSourceRedirectionIndex(redirections);

        this.resultColumns = new LinkedHashMap<>(resultPairs.length);
        for (final MatchPair mp : resultPairs) {
            // noinspection unchecked
            resultColumns.put(mp.left(),
                    new ReadOnlyRedirectedColumnSource(redirectionIndex, originalTable.getColumnSource(mp.right())));
        }
        if (exposeRedirectionAs != null) {
            resultColumns.put(exposeRedirectionAs, redirections);
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // we have no inputs, so should never get here
        throw new IllegalStateException();
    }

    @Override
    public void shiftChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preShiftIndices,
            LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void modifyIndices(BucketedContext context, LongChunk<? extends KeyIndices> inputIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        // we have no inputs, so should never get here
        throw new IllegalStateException();
    }

    @Override
    public boolean shiftChunk(SingletonContext singletonContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preInputIndices,
            LongChunk<? extends KeyIndices> postInputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyIndices(SingletonContext context, LongChunk<? extends KeyIndices> indices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ensureCapacity(long tableSize) {
        redirections.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        redirectionIndex.startTrackingPrevValues();
    }

    @Override
    public boolean requiresIndices() {
        return true;
    }
}
