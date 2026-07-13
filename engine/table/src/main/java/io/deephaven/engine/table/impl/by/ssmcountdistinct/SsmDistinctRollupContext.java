//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedIntChunk;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.util.mutable.MutableInt;

public class SsmDistinctRollupContext implements IterativeChunkedAggregationOperator.SingletonContext {
    public final SegmentedSortedMultiSet.RemoveContext removeContext =
            SegmentedSortedMultiSet.makeRemoveContext(SsmDistinctContext.NODE_SIZE);
    public final SizedChunk<Values> valueCopy;
    public final SizedIntChunk<ChunkLengths> counts;
    // a parallel pair used when diffing a modify into net removals (valueCopy/counts) and net additions
    public final SizedChunk<Values> postValues;
    public final SizedIntChunk<ChunkLengths> postCounts;
    public final MutableInt removedSize = new MutableInt();
    public final MutableInt addedSize = new MutableInt();

    public SsmDistinctRollupContext(ChunkType chunkType) {
        valueCopy = new SizedChunk<>(chunkType);
        counts = new SizedIntChunk<>();
        postValues = new SizedChunk<>(chunkType);
        postCounts = new SizedIntChunk<>();
    }

    @Override
    public void close() {
        valueCopy.close();
        counts.close();
        postValues.close();
        postCounts.close();
    }
}
