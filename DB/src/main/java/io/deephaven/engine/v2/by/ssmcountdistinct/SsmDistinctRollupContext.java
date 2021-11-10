package io.deephaven.engine.v2.by.ssmcountdistinct;

import io.deephaven.engine.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.ChunkType;
import io.deephaven.engine.chunk.sized.SizedChunk;
import io.deephaven.engine.chunk.sized.SizedIntChunk;
import io.deephaven.engine.v2.ssms.SegmentedSortedMultiSet;

public class SsmDistinctRollupContext implements IterativeChunkedAggregationOperator.SingletonContext {
    public final SegmentedSortedMultiSet.RemoveContext removeContext =
            SegmentedSortedMultiSet.makeRemoveContext(DistinctOperatorFactory.NODE_SIZE);
    public final SizedChunk<Attributes.Values> valueCopy;
    public final SizedIntChunk<Attributes.ChunkLengths> counts;

    public SsmDistinctRollupContext(ChunkType chunkType) {
        valueCopy = new SizedChunk<>(chunkType);
        counts = new SizedIntChunk<>();
    }

    @Override
    public void close() {
        valueCopy.close();
        counts.close();
    }
}
