package io.deephaven.db.v2.by.ssmcountdistinct;

import io.deephaven.db.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.ssms.SegmentedSortedMultiSet;

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
