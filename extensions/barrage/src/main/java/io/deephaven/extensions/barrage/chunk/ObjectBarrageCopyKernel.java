//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;

public class ObjectBarrageCopyKernel {
    /**
     * Context for the ObjectBarrageCopyKernel that holds the add / mod chunks as WritableObjectChunk and the delta
     * chunk size.
     */
    private static class ObjectBarrageCopyKernelContext implements BarrageCopyKernel.BarrageCopyKernelContext {
        private final WritableObjectChunk<Object, Values>[][] addChunks;
        private final WritableObjectChunk<Object, Values>[][] modChunks;
        private final int deltaChunkSize;

        private ObjectBarrageCopyKernelContext(
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            // Clone and cast the add / mod chunk arrays to WritableObjectChunk.
            // noinspection unchecked
            this.addChunks = new WritableObjectChunk[addChunks.length][];
            for (int i = 0; i < addChunks.length; i++) {
                if (addChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.addChunks[i] = new WritableObjectChunk[addChunks[i].length];
                for (int j = 0; j < addChunks[i].length; j++) {
                    this.addChunks[i][j] = addChunks[i][j].asWritableObjectChunk();
                }
            }
            // noinspection unchecked
            this.modChunks = new WritableObjectChunk[modChunks.length][];
            for (int i = 0; i < modChunks.length; i++) {
                if (modChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.modChunks[i] = new WritableObjectChunk[modChunks[i].length];
                for (int j = 0; j < modChunks[i].length; j++) {
                    this.modChunks[i][j] = modChunks[i][j].asWritableObjectChunk();
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
            final WritableObjectChunk<Object, Values> dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {

        final ObjectBarrageCopyKernelContext objectContext = (ObjectBarrageCopyKernelContext) context;
        final int deltaChunkSize = objectContext.deltaChunkSize();
        final WritableObjectChunk<Object, Values>[][] addChunks = objectContext.addChunks;
        final WritableObjectChunk<Object, Values>[][] modChunks = objectContext.modChunks;

        for (int pos = 0; pos < mapping.length; ++pos) {
            final long encoded = mapping[pos];
            final boolean fromMods = (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0;
            final int deltaIdx =
                    (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
            final long srcPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;

            final WritableObjectChunk<Object, Values>[] srcChunks =
                    fromMods ? modChunks[deltaIdx] : addChunks[deltaIdx];
            final int srcChunkIdx = (int) (srcPos / deltaChunkSize);
            final int srcOff = (int) (srcPos % deltaChunkSize);
            dest.set(pos, srcChunks[srcChunkIdx].get(srcOff));
        }
    }

    /**
     * Implementation of the ObjectBarrageCopyKernel that delegates to static methods.
     */
    private static class ObjectBarrageCopyKernelImpl implements BarrageCopyKernel {
        @Override
        public BarrageCopyKernelContext makeContext(WritableChunk<Values>[][] addChunks,
                WritableChunk<Values>[][] modChunks, int deltaChunkSize) {
            return new ObjectBarrageCopyKernelContext(addChunks, modChunks, deltaChunkSize);
        }

        @Override
        public void copyFromDeltaChunks(long[] mapping, WritableChunk<Values> dest, BarrageCopyKernelContext context) {
            ObjectBarrageCopyKernel.copyFromDeltaChunks(mapping, dest.asWritableObjectChunk(), context);
        }
    }

    static final BarrageCopyKernel INSTANCE = new ObjectBarrageCopyKernelImpl();
}
