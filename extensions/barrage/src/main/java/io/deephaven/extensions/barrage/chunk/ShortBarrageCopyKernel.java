//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageCopyKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

public class ShortBarrageCopyKernel {
    /**
     * Context for the ShortBarrageCopyKernel that holds the add / mod chunks as WritableShortChunk and the delta chunk
     * size.
     */
    private static class ShortBarrageCopyKernelContext implements BarrageCopyKernel.BarrageCopyKernelContext {
        private final WritableShortChunk<Values>[][] addChunks;
        private final WritableShortChunk<Values>[][] modChunks;
        private final int deltaChunkSize;

        private ShortBarrageCopyKernelContext(
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            // Clone and cast the add / mod chunk arrays to WritableShortChunk.
            // noinspection unchecked
            this.addChunks = new WritableShortChunk[addChunks.length][];
            for (int i = 0; i < addChunks.length; i++) {
                if (addChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.addChunks[i] = new WritableShortChunk[addChunks[i].length];
                for (int j = 0; j < addChunks[i].length; j++) {
                    this.addChunks[i][j] = addChunks[i][j].asWritableShortChunk();
                }
            }
            // noinspection unchecked
            this.modChunks = new WritableShortChunk[modChunks.length][];
            for (int i = 0; i < modChunks.length; i++) {
                if (modChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.modChunks[i] = new WritableShortChunk[modChunks[i].length];
                for (int j = 0; j < modChunks[i].length; j++) {
                    this.modChunks[i][j] = modChunks[i][j].asWritableShortChunk();
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
            final WritableShortChunk<Values> dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {

        final ShortBarrageCopyKernelContext shortContext = (ShortBarrageCopyKernelContext) context;
        final int deltaChunkSize = shortContext.deltaChunkSize();
        final WritableShortChunk<Values>[][] addChunks = shortContext.addChunks;
        final WritableShortChunk<Values>[][] modChunks = shortContext.modChunks;

        for (int pos = 0; pos < mapping.length; ++pos) {
            final long encoded = mapping[pos];
            final boolean fromMods = (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0;
            final int deltaIdx =
                    (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
            final long srcPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;

            final WritableShortChunk<Values>[] srcChunks = fromMods ? modChunks[deltaIdx] : addChunks[deltaIdx];
            final int srcChunkIdx = (int) (srcPos / deltaChunkSize);
            final int srcOff = (int) (srcPos % deltaChunkSize);
            dest.set(pos, srcChunks[srcChunkIdx].get(srcOff));
        }
    }

    /**
     * Implementation of the ShortBarrageCopyKernel that delegates to static methods.
     */
    private static class ShortBarrageCopyKernelImpl implements BarrageCopyKernel {
        @Override
        public BarrageCopyKernelContext makeContext(WritableChunk<Values>[][] addChunks,
                WritableChunk<Values>[][] modChunks, int deltaChunkSize) {
            return new ShortBarrageCopyKernelContext(addChunks, modChunks, deltaChunkSize);
        }

        @Override
        public void copyFromDeltaChunks(long[] mapping, WritableChunk<Values> dest, BarrageCopyKernelContext context) {
            ShortBarrageCopyKernel.copyFromDeltaChunks(mapping, dest.asWritableShortChunk(), context);
        }
    }

    static final BarrageCopyKernel INSTANCE = new ShortBarrageCopyKernelImpl();
}
