package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.chunk.*;

public class BucketSsmDistinctRollupContext extends SsmDistinctRollupContext
        implements IterativeChunkedAggregationOperator.BucketedContext {
    public final WritableIntChunk<ChunkLengths> lengthCopy;
    final WritableIntChunk<ChunkLengths> countCopy;
    public final WritableIntChunk<ChunkPositions> starts;
    public final ResettableWritableChunk<Values> valueResettable;
    public final ResettableWritableIntChunk<ChunkLengths> countResettable;
    public final WritableBooleanChunk<?> ssmsToMaybeClear;

    public BucketSsmDistinctRollupContext(ChunkType chunkType, int size) {
        super(chunkType);
        lengthCopy = WritableIntChunk.makeWritableChunk(size);
        countCopy = WritableIntChunk.makeWritableChunk(size);
        countResettable = ResettableWritableIntChunk.makeResettableChunk();
        starts = WritableIntChunk.makeWritableChunk(size);
        valueResettable = chunkType.makeResettableWritableChunk();
        ssmsToMaybeClear = WritableBooleanChunk.makeWritableChunk(size);
    }

    @Override
    public void close() {
        super.close();
        lengthCopy.close();
        countCopy.close();
        countResettable.close();
        starts.close();
        valueResettable.close();
        ssmsToMaybeClear.close();
    }
}
