//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ResettableLongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;

/**
 * Bulk writes for a {@link ChunkSsaStamp}, which restamps or reports a run of consecutive positions of the left keys
 * chunk for each right row.
 */
final class ChunkSsaStampRuns {
    private ChunkSsaStampRuns() {}

    /**
     * Redirect the left keys at positions {@code [start, end)} to {@code innerRowKey}, a
     * {@link io.deephaven.engine.rowset.RowSequence#NULL_ROW_KEY} removing their mappings, and add them to
     * {@code modifiedBuilder}.
     */
    static void restamp(final LongChunk<RowKeys> leftStampKeys, final int start, final int end,
            final long innerRowKey, final WritableRowRedirection rowRedirection,
            final RowSetBuilderRandom modifiedBuilder) {
        final int runLength = end - start;
        if (runLength == 0) {
            return;
        }
        // the inner row keys of a run are one value, so a pooled chunk of them serves every slice of the run
        final int sliceCapacity = Math.min(runLength, ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY);
        try (final WritableLongChunk<RowKeys> innerRowKeys = WritableLongChunk.makeWritableChunk(sliceCapacity);
                final ResettableLongChunk<RowKeys> outerRowKeys = ResettableLongChunk.makeResettableChunk();
                final ChunkSink.FillFromContext fillFromContext = rowRedirection.makeFillFromContext(sliceCapacity)) {
            innerRowKeys.fillWithValue(0, sliceCapacity, innerRowKey);
            for (int sliceStart = start; sliceStart < end; sliceStart += sliceCapacity) {
                final int sliceLength = Math.min(sliceCapacity, end - sliceStart);
                innerRowKeys.setSize(sliceLength);
                outerRowKeys.resetFromTypedChunk(leftStampKeys, sliceStart, sliceLength);
                rowRedirection.fillFromChunkUnordered(fillFromContext, innerRowKeys, outerRowKeys);
            }
        }
        addModified(leftStampKeys, start, end, modifiedBuilder);
    }

    /**
     * Add the left keys at positions {@code [start, end)} to {@code modifiedBuilder}.
     */
    static void addModified(final LongChunk<RowKeys> leftStampKeys, final int start, final int end,
            final RowSetBuilderRandom modifiedBuilder) {
        if (end - start == 0) {
            return;
        }
        // the left keys are ordered by stamp, which usually orders them by row key as well
        for (int ii = start + 1; ii < end; ++ii) {
            if (leftStampKeys.get(ii) <= leftStampKeys.get(ii - 1)) {
                for (int jj = start; jj < end; ++jj) {
                    modifiedBuilder.addKey(leftStampKeys.get(jj));
                }
                return;
            }
        }
        modifiedBuilder.addOrderedRowKeysChunk(LongChunk.downcast(leftStampKeys), start, end - start);
    }
}
