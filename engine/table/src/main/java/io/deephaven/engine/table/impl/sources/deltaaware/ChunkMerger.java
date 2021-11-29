package io.deephaven.engine.table.impl.sources.deltaaware;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;

class ChunkMerger<ATTR extends Any> {
    // Copy the data back into the positions where it needs to go.
    static <ATTR extends Any> void merge(
            Chunk<? extends ATTR> bChunk, Chunk<? extends ATTR> dChunk,
            RowSequence bKeys, RowSequence dKeys,
            WritableChunk<? super ATTR> dest) {
        final ChunkMerger<ATTR> bMerger = new ChunkMerger<>(bChunk, bKeys);
        final ChunkMerger<ATTR> dMerger = new ChunkMerger<>(dChunk, dKeys);

        int destOffset = 0;
        while (true) {
            final int destOffsetAtStart = destOffset;
            destOffset = bMerger.copyIfYouCan(dest, destOffset, dMerger);
            destOffset = dMerger.copyIfYouCan(dest, destOffset, bMerger);
            if (destOffset == destOffsetAtStart) {
                // No progress on either side. It seems I am done.
                break;
            }
        }
        dest.setSize(destOffset);
    }

    private final Chunk<? extends ATTR> src;
    private final LongChunk<OrderedRowKeyRanges> keyRanges;
    private int keyOffset;
    private int dataOffset;

    private ChunkMerger(Chunk<? extends ATTR> src, RowSequence keys) {
        this.src = src;
        keyRanges = keys.asRowKeyRangesChunk();
        keyOffset = 0;
        dataOffset = 0;
    }

    /**
     * @return New destOffset. If the new offset is the same as the input parameter, then I did no work.
     */
    private int copyIfYouCan(WritableChunk<? super ATTR> dest, int destOffset, ChunkMerger<ATTR> other) {
        int contiguousSize = 0;

        final long otherFirst =
                other.keyOffset == other.keyRanges.size() ? Long.MAX_VALUE : other.keyRanges.get(other.keyOffset);

        while (true) {
            if (keyOffset == keyRanges.size()) {
                // Get out because my keys are exhausted
                break;
            }
            final long rangeFirst = keyRanges.get(keyOffset);
            final long rangeLast = keyRanges.get(keyOffset + 1);
            if (rangeFirst > otherFirst) {
                // Get out because both (myself and my other) have keys but next smallest key is not mine
                break;
            }
            contiguousSize += rangeLast - rangeFirst + 1;
            keyOffset += 2;
        }

        // And copy them
        if (contiguousSize > 0) {
            dest.copyFromChunk(src, dataOffset, destOffset, contiguousSize);
            dataOffset += contiguousSize;
            destOffset += contiguousSize;
        }
        return destOffset;
    }
}
