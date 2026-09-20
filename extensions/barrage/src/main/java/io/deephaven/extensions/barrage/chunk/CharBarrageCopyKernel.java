//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

public class CharBarrageCopyKernel {
    /**
     * Context for the CharBarrageCopyKernel that holds the add / mod chunks as WritableCharChunk and the delta chunk
     * size.
     */
    private static class CharBarrageCopyKernelContext implements BarrageCopyKernel.BarrageCopyKernelContext {
        private final WritableCharChunk<Values>[][] addChunks;
        private final WritableCharChunk<Values>[][] modChunks;
        private final int deltaChunkSize;

        private CharBarrageCopyKernelContext(
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            // Clone and cast the add / mod chunk arrays to WritableCharChunk.
            // noinspection unchecked
            this.addChunks = new WritableCharChunk[addChunks.length][];
            for (int i = 0; i < addChunks.length; i++) {
                if (addChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.addChunks[i] = new WritableCharChunk[addChunks[i].length];
                for (int j = 0; j < addChunks[i].length; j++) {
                    this.addChunks[i][j] = addChunks[i][j].asWritableCharChunk();
                }
            }
            // noinspection unchecked
            this.modChunks = new WritableCharChunk[modChunks.length][];
            for (int i = 0; i < modChunks.length; i++) {
                if (modChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.modChunks[i] = new WritableCharChunk[modChunks[i].length];
                for (int j = 0; j < modChunks[i].length; j++) {
                    this.modChunks[i][j] = modChunks[i][j].asWritableCharChunk();
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
    private static WritableCharChunk<Values>[] originChunks(
            final long encoded,
            final WritableCharChunk<Values>[][] addChunks,
            final WritableCharChunk<Values>[][] modChunks) {
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
            final WritableCharChunk<Values>[] dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {
        // hoisted out of the loops
        final CharBarrageCopyKernelContext charContext = (CharBarrageCopyKernelContext) context;
        final int deltaChunkSize = charContext.deltaChunkSize;
        final WritableCharChunk<Values>[][] addChunks = charContext.addChunks;
        final WritableCharChunk<Values>[][] modChunks = charContext.modChunks;
        for (int ri = 0; ri < runs.count; ++ri) {
            final long encoded = runs.encoded[ri];
            final WritableCharChunk<Values>[] originChunks = originChunks(encoded, addChunks, modChunks);

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
     * Implementation of the CharBarrageCopyKernel that delegates to static methods.
     */
    private static class CharBarrageCopyKernelImpl implements BarrageCopyKernel {
        @Override
        public BarrageCopyKernelContext makeContext(WritableChunk<Values>[][] addChunks,
                WritableChunk<Values>[][] modChunks, int deltaChunkSize) {
            return new CharBarrageCopyKernelContext(addChunks, modChunks, deltaChunkSize);
        }

        @Override
        public void copy(Runs runs, WritableChunk<Values>[] dest, BarrageCopyKernelContext context) {
            // noinspection unchecked
            final WritableCharChunk<Values>[] typedDest = new WritableCharChunk[dest.length];
            for (int ii = 0; ii < dest.length; ++ii) {
                typedDest[ii] = dest[ii].asWritableCharChunk();
            }
            CharBarrageCopyKernel.copy(runs, typedDest, context);
        }
    }

    static final BarrageCopyKernel INSTANCE = new CharBarrageCopyKernelImpl();
}
