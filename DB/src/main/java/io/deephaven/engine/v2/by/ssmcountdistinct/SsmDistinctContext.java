package io.deephaven.engine.v2.by.ssmcountdistinct;

import io.deephaven.engine.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.ChunkType;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableIntChunk;
import io.deephaven.engine.v2.ssms.SegmentedSortedMultiSet;

public class SsmDistinctContext implements IterativeChunkedAggregationOperator.SingletonContext {
    public final SegmentedSortedMultiSet.RemoveContext removeContext =
            SegmentedSortedMultiSet.makeRemoveContext(DistinctOperatorFactory.NODE_SIZE);
    public final WritableChunk<Attributes.Values> valueCopy;
    public final WritableIntChunk<Attributes.ChunkLengths> counts;

    public SsmDistinctContext(ChunkType chunkType, int size) {
        valueCopy = chunkType.makeWritableChunk(size);
        counts = WritableIntChunk.makeWritableChunk(size);
    }

    @Override
    public void close() {
        valueCopy.close();
        counts.close();
    }
}
