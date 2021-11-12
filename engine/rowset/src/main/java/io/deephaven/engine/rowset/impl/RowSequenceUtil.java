package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.WritableLongChunk;

/**
 * Helper methods for constructing {@link RowSequence} instances.
 */
public class RowSequenceUtil {
    /**
     * Wrap a LongChunk as an {@link RowSequence}.
     * 
     * @param longChunk A {@link LongChunk chunk} to wrap as a new {@link RowSequence} object.
     * @return A new {@link RowSequence} object, who does not own the passed chunk.
     */
    public static RowSequence wrapRowKeysChunkAsRowSequence(final LongChunk<Attributes.OrderedRowKeys> longChunk) {
        return RowSequenceRowKeysChunkImpl.makeByWrapping(longChunk);
    }

    /**
     * Wrap a LongChunk as an {@link RowSequence}.
     * 
     * @param longChunk A {@link LongChunk chunk} to wrap as a new {@link RowSequence} object.
     * @return A new {@link RowSequence} object, who does not own the passed chunk.
     */
    public static RowSequence wrapKeyRangesChunkAsRowSequence(
            final LongChunk<Attributes.OrderedRowKeyRanges> longChunk) {
        return RowSequenceKeyRangesChunkImpl.makeByWrapping(longChunk);
    }

    /**
     * Create and return a new {@link RowSequence} object from the provided WritableLongChunk.
     * 
     * @param longChunk The input {@link WritableLongChunk chunk}. The returned object will take ownership of this
     *        chunk.
     * @return A new {@link RowSequence} object, who owns the passed chunk.
     */
    public static RowSequence takeRowKeysChunkAndMakeRowSequence(
            final WritableLongChunk<Attributes.OrderedRowKeys> longChunk) {
        return RowSequenceRowKeysChunkImpl.makeByTaking(longChunk);
    }

    /**
     * Create and return a new {@link RowSequence} object from the provided WritableLongChunk.
     * 
     * @param longChunk The input {@link WritableLongChunk chunk}. The returned object will take ownership of this
     *        chunk.
     * @return A new {@link RowSequence} object, who owns the passed chunk.
     */
    public static RowSequence takeKeyRangesChunkAndMakeRowSequence(
            final WritableLongChunk<Attributes.OrderedRowKeyRanges> longChunk) {
        return RowSequenceKeyRangesChunkImpl.makeByTaking(longChunk);
    }

    /**
     * Create and return a new {@link RowSequence} object from the supplied closed range.
     *
     * @param firstRowKey The first row key (inclusive) in the range
     * @param lastRowKey The last row key (inclusive) in the range
     * @return A new {@link RowSequence} object covering the requested range of row keys
     */
    public static RowSequence forRange(final long firstRowKey, final long lastRowKey) {
        // NB: We could use a pooled chunk, here, but the pool doesn't usually
        // hold chunks this small. Probably not worth the code complexity for release, either.
        return wrapKeyRangesChunkAsRowSequence(LongChunk.chunkWrap(new long[] {firstRowKey, lastRowKey}));
    }
}
