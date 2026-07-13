//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.util.mutable.MutableInt;

public class SsmDistinctContext implements IterativeChunkedAggregationOperator.SingletonContext {

    public static final int NODE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("SsmDistinctContext.nodeSize", 4096);

    public final SegmentedSortedMultiSet.RemoveContext removeContext =
            SegmentedSortedMultiSet.makeRemoveContext(NODE_SIZE);
    public final WritableChunk<Values> valueCopy;
    public final WritableIntChunk<ChunkLengths> counts;
    // a parallel pair used when diffing a modify into net removals (valueCopy/counts) and net additions
    public final WritableChunk<Values> postValues;
    public final WritableIntChunk<ChunkLengths> postCounts;
    public final MutableInt removedSize = new MutableInt();
    public final MutableInt addedSize = new MutableInt();

    public SsmDistinctContext(ChunkType chunkType, int size) {
        valueCopy = chunkType.makeWritableChunk(size);
        counts = WritableIntChunk.makeWritableChunk(size);
        postValues = chunkType.makeWritableChunk(size);
        postCounts = WritableIntChunk.makeWritableChunk(size);
    }

    @Override
    public void close() {
        valueCopy.close();
        counts.close();
        postValues.close();
        postCounts.close();
    }
}
