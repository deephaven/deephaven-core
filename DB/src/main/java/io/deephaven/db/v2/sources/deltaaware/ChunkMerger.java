package io.deephaven.db.v2.sources.deltaaware;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.OrderedKeys;

class ChunkMerger<ATTR extends Attributes.Any> {
    // Copy the data back into the positions where it needs to go.
    static <ATTR extends Attributes.Any> void merge(Chunk<ATTR> bChunk, Chunk<ATTR> dChunk,
        OrderedKeys bKeys,
        OrderedKeys dKeys, WritableChunk<? super ATTR> dest) {
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

    private final Chunk<ATTR> src;
    private final LongChunk<Attributes.OrderedKeyRanges> keyRanges;
    private int keyOffset;
    private int dataOffset;

    private ChunkMerger(Chunk<ATTR> src, OrderedKeys keys) {
        this.src = src;
        keyRanges = keys.asKeyRangesChunk();
        keyOffset = 0;
        dataOffset = 0;
    }

    /**
     * @return New destOffset. If the new offset is the same as the input parameter, then I did no
     *         work.
     */
    private int copyIfYouCan(WritableChunk<? super ATTR> dest, int destOffset, ChunkMerger other) {
        int contiguousSize = 0;

        final long otherFirst = other.keyOffset == other.keyRanges.size() ? Long.MAX_VALUE
            : other.keyRanges.get(other.keyOffset);

        while (true) {
            if (keyOffset == keyRanges.size()) {
                // Get out because my keys are exhausted
                break;
            }
            final long rangeFirst = keyRanges.get(keyOffset);
            final long rangeLast = keyRanges.get(keyOffset + 1);
            if (rangeFirst > otherFirst) {
                // Get out because both (myself and my other) have keys but next smallest key is not
                // mine
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
