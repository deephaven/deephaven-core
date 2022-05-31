package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.chunk.*;

public class BucketSsmDistinctContext extends SsmDistinctContext
        implements IterativeChunkedAggregationOperator.BucketedContext {
    public final WritableIntChunk<ChunkLengths> lengthCopy;
    public final ResettableWritableChunk<Values> valueResettable;
    public final ResettableWritableIntChunk<ChunkLengths> countResettable;
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
