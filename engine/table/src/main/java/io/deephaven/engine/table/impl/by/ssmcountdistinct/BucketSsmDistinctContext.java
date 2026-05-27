//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.chunk.*;

public class BucketSsmDistinctContext extends SsmDistinctContext
        implements IterativeChunkedAggregationOperator.BucketedContext {
    public final WritableIntChunk<ChunkLengths> lengthCopy;
    public final WritableBooleanChunk<?> ssmsToMaybeClear;

    public BucketSsmDistinctContext(ChunkType chunkType, int size) {
        super(chunkType, size);
        lengthCopy = WritableIntChunk.makeWritableChunk(size);
        ssmsToMaybeClear = WritableBooleanChunk.makeWritableChunk(size);
    }

    @Override
    public void close() {
        super.close();
        lengthCopy.close();
        ssmsToMaybeClear.close();
    }
}
