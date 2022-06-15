/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

public class SsmDistinctContext implements IterativeChunkedAggregationOperator.SingletonContext {

    public static final int NODE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("SsmDistinctContext.nodeSize", 4096);

    public final SegmentedSortedMultiSet.RemoveContext removeContext =
            SegmentedSortedMultiSet.makeRemoveContext(NODE_SIZE);
    public final WritableChunk<Values> valueCopy;
    public final WritableIntChunk<ChunkLengths> counts;

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
