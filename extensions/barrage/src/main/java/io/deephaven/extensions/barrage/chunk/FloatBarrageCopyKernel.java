//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageCopyKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

public class FloatBarrageCopyKernel {
    /**
     * Context for the FloatBarrageCopyKernel that holds the add / mod chunks as WritableFloatChunk and the delta chunk
     * size.
     */
    private static class FloatBarrageCopyKernelContext implements BarrageCopyKernel.BarrageCopyKernelContext {
        private final WritableFloatChunk<Values>[][] addChunks;
        private final WritableFloatChunk<Values>[][] modChunks;
        private final int deltaChunkSize;

        private FloatBarrageCopyKernelContext(
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            // Clone and cast the add / mod chunk arrays to WritableFloatChunk.
            // noinspection unchecked
            this.addChunks = new WritableFloatChunk[addChunks.length][];
            for (int i = 0; i < addChunks.length; i++) {
                if (addChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.addChunks[i] = new WritableFloatChunk[addChunks[i].length];
                for (int j = 0; j < addChunks[i].length; j++) {
                    this.addChunks[i][j] = addChunks[i][j].asWritableFloatChunk();
                }
            }
            // noinspection unchecked
            this.modChunks = new WritableFloatChunk[modChunks.length][];
            for (int i = 0; i < modChunks.length; i++) {
                if (modChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.modChunks[i] = new WritableFloatChunk[modChunks[i].length];
                for (int j = 0; j < modChunks[i].length; j++) {
                    this.modChunks[i][j] = modChunks[i][j].asWritableFloatChunk();
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
            final WritableFloatChunk<Values> dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {

        final FloatBarrageCopyKernelContext floatContext = (FloatBarrageCopyKernelContext) context;
        final int deltaChunkSize = floatContext.deltaChunkSize();
        final WritableFloatChunk<Values>[][] addChunks = floatContext.addChunks;
        final WritableFloatChunk<Values>[][] modChunks = floatContext.modChunks;

        for (int pos = 0; pos < mapping.length; ++pos) {
            final long encoded = mapping[pos];
            final boolean fromMods = (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0;
            final int deltaIdx =
                    (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
            final long srcPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;

            final WritableFloatChunk<Values>[] srcChunks = fromMods ? modChunks[deltaIdx] : addChunks[deltaIdx];
            final int srcChunkIdx = (int) (srcPos / deltaChunkSize);
            final int srcOff = (int) (srcPos % deltaChunkSize);
            dest.set(pos, srcChunks[srcChunkIdx].get(srcOff));
        }
    }

    /**
     * Implementation of the FloatBarrageCopyKernel that delegates to static methods.
     */
    private static class FloatBarrageCopyKernelImpl implements BarrageCopyKernel {
        @Override
        public BarrageCopyKernelContext makeContext(WritableChunk<Values>[][] addChunks,
                WritableChunk<Values>[][] modChunks, int deltaChunkSize) {
            return new FloatBarrageCopyKernelContext(addChunks, modChunks, deltaChunkSize);
        }

        @Override
        public void copyFromDeltaChunks(long[] mapping, WritableChunk<Values> dest, BarrageCopyKernelContext context) {
            FloatBarrageCopyKernel.copyFromDeltaChunks(mapping, dest.asWritableFloatChunk(), context);
        }
    }

    static final BarrageCopyKernel INSTANCE = new FloatBarrageCopyKernelImpl();
}
