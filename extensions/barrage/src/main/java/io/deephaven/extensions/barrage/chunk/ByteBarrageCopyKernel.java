//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageCopyKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

public class ByteBarrageCopyKernel {
    /**
     * Context for the ByteBarrageCopyKernel that holds the add / mod chunks as WritableByteChunk and the delta chunk
     * size.
     */
    private static class ByteBarrageCopyKernelContext implements BarrageCopyKernel.BarrageCopyKernelContext {
        private final WritableByteChunk<Values>[][] addChunks;
        private final WritableByteChunk<Values>[][] modChunks;
        private final int deltaChunkSize;

        private ByteBarrageCopyKernelContext(
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            // Clone and cast the add / mod chunk arrays to WritableByteChunk.
            // noinspection unchecked
            this.addChunks = new WritableByteChunk[addChunks.length][];
            for (int i = 0; i < addChunks.length; i++) {
                if (addChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.addChunks[i] = new WritableByteChunk[addChunks[i].length];
                for (int j = 0; j < addChunks[i].length; j++) {
                    this.addChunks[i][j] = addChunks[i][j].asWritableByteChunk();
                }
            }
            // noinspection unchecked
            this.modChunks = new WritableByteChunk[modChunks.length][];
            for (int i = 0; i < modChunks.length; i++) {
                if (modChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.modChunks[i] = new WritableByteChunk[modChunks[i].length];
                for (int j = 0; j < modChunks[i].length; j++) {
                    this.modChunks[i][j] = modChunks[i][j].asWritableByteChunk();
                }
            }
            this.deltaChunkSize = deltaChunkSize;
        }

        @Override
        public int deltaChunkSize() {
            return deltaChunkSize;
        }
    }

    /**
     * Copy values from the delta chunks into the destination chunk according to the mapping. Each mapping entry encodes
     * the source delta chunk and position, and whether it comes from an add or mod chunk. This method decodes the
     * mapping and performs the copy for each position in the mapping.
     */
    private static void copyFromDeltaChunks(
            final long[] mapping,
            final WritableByteChunk<Values> dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {

        final ByteBarrageCopyKernelContext byteContext = (ByteBarrageCopyKernelContext) context;
        final int deltaChunkSize = byteContext.deltaChunkSize();
        final WritableByteChunk<Values>[][] addChunks = byteContext.addChunks;
        final WritableByteChunk<Values>[][] modChunks = byteContext.modChunks;

        for (int pos = 0; pos < mapping.length; ++pos) {
            final long encoded = mapping[pos];
            final boolean fromMods = (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0;
            final int deltaIdx =
                    (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
            final long srcPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;

            final WritableByteChunk<Values>[] srcChunks = fromMods ? modChunks[deltaIdx] : addChunks[deltaIdx];
            final int srcChunkIdx = (int) (srcPos / deltaChunkSize);
            final int srcOff = (int) (srcPos % deltaChunkSize);
            dest.set(pos, srcChunks[srcChunkIdx].get(srcOff));
        }
    }

    /**
     * Implementation of the ByteBarrageCopyKernel that delegates to static methods.
     */
    private static class ByteBarrageCopyKernelImpl implements BarrageCopyKernel {
        @Override
        public BarrageCopyKernelContext makeContext(WritableChunk<Values>[][] addChunks,
                WritableChunk<Values>[][] modChunks, int deltaChunkSize) {
            return new ByteBarrageCopyKernelContext(addChunks, modChunks, deltaChunkSize);
        }

        @Override
        public void copyFromDeltaChunks(long[] mapping, WritableChunk<Values> dest, BarrageCopyKernelContext context) {
            ByteBarrageCopyKernel.copyFromDeltaChunks(mapping, dest.asWritableByteChunk(), context);
        }
    }

    static final BarrageCopyKernel INSTANCE = new ByteBarrageCopyKernelImpl();
}
