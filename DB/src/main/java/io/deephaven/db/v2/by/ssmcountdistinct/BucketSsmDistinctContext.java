package io.deephaven.db.v2.by.ssmcountdistinct;

import io.deephaven.db.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.db.v2.sources.chunk.*;

public class BucketSsmDistinctContext extends SsmDistinctContext
        implements IterativeChunkedAggregationOperator.BucketedContext {
    public final WritableIntChunk<Attributes.ChunkLengths> lengthCopy;
    public final ResettableWritableChunk<Attributes.Values> valueResettable;
    public final ResettableWritableIntChunk<Attributes.ChunkLengths> countResettable;
    public final WritableBooleanChunk<?> ssmsToMaybeClear;

    public BucketSsmDistinctContext(ChunkType chunkType, int size) {
        super(chunkType, size);
        lengthCopy = WritableIntChunk.makeWritableChunk(size);
        valueResettable = chunkType.makeResettableWritableChunk();
        countResettable = ResettableWritableIntChunk.makeResettableChunk();
        ssmsToMaybeClear = WritableBooleanChunk.makeWritableChunk(size);
    }

    @Override
    public void close() {
        super.close();
        lengthCopy.close();
        valueResettable.close();
        countResettable.close();
        ssmsToMaybeClear.close();
    }
}
