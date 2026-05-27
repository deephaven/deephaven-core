//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.chunk.*;

public class BucketSsmDistinctRollupContext extends SsmDistinctRollupContext
        implements IterativeChunkedAggregationOperator.BucketedContext {
    public final WritableIntChunk<ChunkLengths> lengthCopy;
    final WritableIntChunk<ChunkLengths> countCopy;
    public final WritableIntChunk<ChunkPositions> starts;
    // per-bucket starts/lengths for the net-addition side (postValues) when diffing a modify
    public final WritableIntChunk<ChunkPositions> postStarts;
    public final WritableIntChunk<ChunkLengths> postLengthCopy;

    public BucketSsmDistinctRollupContext(ChunkType chunkType, int size) {
        super(chunkType);
        lengthCopy = WritableIntChunk.makeWritableChunk(size);
        countCopy = WritableIntChunk.makeWritableChunk(size);
        starts = WritableIntChunk.makeWritableChunk(size);
        postStarts = WritableIntChunk.makeWritableChunk(size);
        postLengthCopy = WritableIntChunk.makeWritableChunk(size);
    }

    @Override
    public void close() {
        super.close();
        lengthCopy.close();
        countCopy.close();
        starts.close();
        postStarts.close();
        postLengthCopy.close();
    }
}
