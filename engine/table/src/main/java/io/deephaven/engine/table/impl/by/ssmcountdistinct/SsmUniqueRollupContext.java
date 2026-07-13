//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.util.mutable.MutableInt;

/**
 * Scratch context for the rollup "unique" operator. Per destination it gathers the unique constituent values to add to
 * (or remove from) that destination's multiset into {@link #values}, then compacts them (into distinct values +
 * {@link #counts}) so they can be inserted into or removed from the SSM in a single bulk call rather than one
 * constituent at a time. A modify gathers its removals into {@link #values}/{@link #counts} and its additions into
 * {@link #postValues}/{@link #postCounts}, then nets the two so values unchanged across the modify cancel out before
 * touching the SSM ({@link #removedSize}/{@link #addedSize} carry the surviving lengths).
 */
public class SsmUniqueRollupContext
        implements IterativeChunkedAggregationOperator.BucketedContext,
        IterativeChunkedAggregationOperator.SingletonContext {
    public final SegmentedSortedMultiSet.RemoveContext removeContext =
            SegmentedSortedMultiSet.makeRemoveContext(SsmDistinctContext.NODE_SIZE);
    public final WritableChunk<Values> values;
    public final WritableIntChunk<ChunkLengths> counts;
    // a parallel pair holding a modify's additions while values/counts hold its removals, so the two can be netted
    public final WritableChunk<Values> postValues;
    public final WritableIntChunk<ChunkLengths> postCounts;
    public final MutableInt removedSize = new MutableInt();
    public final MutableInt addedSize = new MutableInt();

    public SsmUniqueRollupContext(ChunkType chunkType, int size) {
        values = chunkType.makeWritableChunk(size);
        counts = WritableIntChunk.makeWritableChunk(size);
        postValues = chunkType.makeWritableChunk(size);
        postCounts = WritableIntChunk.makeWritableChunk(size);
    }

    @Override
    public void close() {
        values.close();
        counts.close();
        postValues.close();
        postCounts.close();
    }
}
