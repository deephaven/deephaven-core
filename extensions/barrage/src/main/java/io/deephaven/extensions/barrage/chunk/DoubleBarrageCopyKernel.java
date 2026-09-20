//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageCopyKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

public class DoubleBarrageCopyKernel {
    /**
     * Context for the DoubleBarrageCopyKernel that holds the add / mod chunks as WritableDoubleChunk and the delta chunk
     * size.
     */
    private static class DoubleBarrageCopyKernelContext implements BarrageCopyKernel.BarrageCopyKernelContext {
        private final WritableDoubleChunk<Values>[][] addChunks;
        private final WritableDoubleChunk<Values>[][] modChunks;
        private final int deltaChunkSize;

        private DoubleBarrageCopyKernelContext(
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            // Clone and cast the add / mod chunk arrays to WritableDoubleChunk.
            // noinspection unchecked
            this.addChunks = new WritableDoubleChunk[addChunks.length][];
            for (int i = 0; i < addChunks.length; i++) {
                if (addChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.addChunks[i] = new WritableDoubleChunk[addChunks[i].length];
                for (int j = 0; j < addChunks[i].length; j++) {
                    this.addChunks[i][j] = addChunks[i][j].asWritableDoubleChunk();
                }
            }
            // noinspection unchecked
            this.modChunks = new WritableDoubleChunk[modChunks.length][];
            for (int i = 0; i < modChunks.length; i++) {
                if (modChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.modChunks[i] = new WritableDoubleChunk[modChunks[i].length];
                for (int j = 0; j < modChunks[i].length; j++) {
                    this.modChunks[i][j] = modChunks[i][j].asWritableDoubleChunk();
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
    private static WritableDoubleChunk<Values>[] originChunks(
            final long encoded,
            final WritableDoubleChunk<Values>[][] addChunks,
            final WritableDoubleChunk<Values>[][] modChunks) {
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
            final WritableDoubleChunk<Values>[] dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {
        // hoisted out of the loops
        final DoubleBarrageCopyKernelContext doubleContext = (DoubleBarrageCopyKernelContext) context;
        final int deltaChunkSize = doubleContext.deltaChunkSize;
        final WritableDoubleChunk<Values>[][] addChunks = doubleContext.addChunks;
        final WritableDoubleChunk<Values>[][] modChunks = doubleContext.modChunks;
        for (int ri = 0; ri < runs.count; ++ri) {
            final long encoded = runs.encoded[ri];
            final WritableDoubleChunk<Values>[] originChunks = originChunks(encoded, addChunks, modChunks);

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
     * Implementation of the DoubleBarrageCopyKernel that delegates to static methods.
     */
    private static class DoubleBarrageCopyKernelImpl implements BarrageCopyKernel {
        @Override
        public BarrageCopyKernelContext makeContext(WritableChunk<Values>[][] addChunks,
                WritableChunk<Values>[][] modChunks, int deltaChunkSize) {
            return new DoubleBarrageCopyKernelContext(addChunks, modChunks, deltaChunkSize);
        }

        @Override
        public void copy(Runs runs, WritableChunk<Values>[] dest, BarrageCopyKernelContext context) {
            // noinspection unchecked
            final WritableDoubleChunk<Values>[] typedDest = new WritableDoubleChunk[dest.length];
            for (int ii = 0; ii < dest.length; ++ii) {
                typedDest[ii] = dest[ii].asWritableDoubleChunk();
            }
            DoubleBarrageCopyKernel.copy(runs, typedDest, context);
        }
    }

    static final BarrageCopyKernel INSTANCE = new DoubleBarrageCopyKernelImpl();
}
