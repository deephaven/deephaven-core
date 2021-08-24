package io.deephaven.db.v2.by.ssmcountdistinct;

import io.deephaven.db.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.db.v2.sources.chunk.*;

public class BucketSsmDistinctRollupContext extends SsmDistinctRollupContext
    implements IterativeChunkedAggregationOperator.BucketedContext {
    public final WritableIntChunk<Attributes.ChunkLengths> lengthCopy;
    final WritableIntChunk<Attributes.ChunkLengths> countCopy;
    public final WritableIntChunk<Attributes.ChunkPositions> starts;
    public final ResettableWritableChunk<Attributes.Values> valueResettable;
    public final ResettableWritableIntChunk<Attributes.ChunkLengths> countResettable;
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
