//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageCopyKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

public class ObjectBarrageCopyKernel {
    /**
     * Context for the ObjectBarrageCopyKernel that holds the add / mod chunks as WritableObjectChunk and the delta chunk
     * size.
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
     * The chunks of one side of one delta, selected by a run's encoded origin.
     */
    private static WritableObjectChunk<Object, Values>[] originChunks(
            final long encoded,
            final WritableObjectChunk<Object, Values>[][] addChunks,
            final WritableObjectChunk<Object, Values>[][] modChunks) {
        final int deltaIdx =
                (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
        return (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0
                ? modChunks[deltaIdx]
                : addChunks[deltaIdx];
    }

    /**
     * Fill the output chunks from the delta chunks according to the runs, splitting a run wherever it crosses an origin
     * or destination chunk boundary and moving each resulting stretch with one typed array copy.
     */
    private static void copy(
            final BarrageCopyKernel.Runs runs,
            final WritableObjectChunk<Object, Values>[] dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {
        // hoisted out of the loops
        final ObjectBarrageCopyKernelContext objectContext = (ObjectBarrageCopyKernelContext) context;
        final int deltaChunkSize = objectContext.deltaChunkSize;
        final WritableObjectChunk<Object, Values>[][] addChunks = objectContext.addChunks;
        final WritableObjectChunk<Object, Values>[][] modChunks = objectContext.modChunks;
        for (int ri = 0; ri < runs.count; ++ri) {
            final long encoded = runs.encoded[ri];
            final WritableObjectChunk<Object, Values>[] originChunks = originChunks(encoded, addChunks, modChunks);

            long originPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;
            long destPos = runs.dest[ri];
            long remaining = runs.len[ri];
            while (remaining > 0) {
                final int originOff = (int) (originPos % deltaChunkSize);
                final int destOff = (int) (destPos % deltaChunkSize);
                final int length = (int) Math.min(remaining,
                        Math.min(deltaChunkSize - originOff, deltaChunkSize - destOff));
                dest[(int) (destPos / deltaChunkSize)].copyFromTypedChunk(
                        originChunks[(int) (originPos / deltaChunkSize)], originOff, destOff, length);
                originPos += length;
                destPos += length;
                remaining -= length;
            }
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
        public void copy(Runs runs, WritableChunk<Values>[] dest, BarrageCopyKernelContext context) {
            // noinspection unchecked
            final WritableObjectChunk<Object, Values>[] typedDest = new WritableObjectChunk[dest.length];
            for (int ii = 0; ii < dest.length; ++ii) {
                typedDest[ii] = dest[ii].asWritableObjectChunk();
            }
            ObjectBarrageCopyKernel.copy(runs, typedDest, context);
        }
    }

    static final BarrageCopyKernel INSTANCE = new ObjectBarrageCopyKernelImpl();
}
