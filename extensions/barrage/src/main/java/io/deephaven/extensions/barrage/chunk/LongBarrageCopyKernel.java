//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageCopyKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

public class LongBarrageCopyKernel {
    /**
     * Context for the LongBarrageCopyKernel that holds the add / mod chunks as WritableLongChunk and the delta chunk
     * size.
     */
    private static class LongBarrageCopyKernelContext implements BarrageCopyKernel.BarrageCopyKernelContext {
        private final WritableLongChunk<Values>[][] addChunks;
        private final WritableLongChunk<Values>[][] modChunks;
        private final int deltaChunkSize;

        private LongBarrageCopyKernelContext(
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            // Clone and cast the add / mod chunk arrays to WritableLongChunk.
            // noinspection unchecked
            this.addChunks = new WritableLongChunk[addChunks.length][];
            for (int i = 0; i < addChunks.length; i++) {
                if (addChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.addChunks[i] = new WritableLongChunk[addChunks[i].length];
                for (int j = 0; j < addChunks[i].length; j++) {
                    this.addChunks[i][j] = addChunks[i][j].asWritableLongChunk();
                }
            }
            // noinspection unchecked
            this.modChunks = new WritableLongChunk[modChunks.length][];
            for (int i = 0; i < modChunks.length; i++) {
                if (modChunks[i] == null) {
                    continue;
                }
                // noinspection unchecked
                this.modChunks[i] = new WritableLongChunk[modChunks[i].length];
                for (int j = 0; j < modChunks[i].length; j++) {
                    this.modChunks[i][j] = modChunks[i][j].asWritableLongChunk();
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
    private static WritableLongChunk<Values>[] originChunks(
            final long encoded,
            final WritableLongChunk<Values>[][] addChunks,
            final WritableLongChunk<Values>[][] modChunks) {
        final int deltaIdx =
                (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
        return (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0
                ? modChunks[deltaIdx]
                : addChunks[deltaIdx];
    }

    /**
     * Copy every run, splitting a run wherever it crosses an origin or destination chunk boundary and moving each
     * resulting stretch with one typed array copy. No length test is needed here, because
     * {@code copyFromTypedChunk} is itself size aware: it uses {@code System.arraycopy} for a stretch of
     * {@code Chunk.SYSTEM_ARRAYCOPY_THRESHOLD} rows or more and an element loop below that, so a short stretch never
     * pays for a call it cannot amortize.
     */
    private static void copy(
            final BarrageCopyKernel.Runs runs,
            final WritableLongChunk<Values>[] dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {
        final LongBarrageCopyKernelContext longContext = (LongBarrageCopyKernelContext) context;
        final int deltaChunkSize = longContext.deltaChunkSize();
        final WritableLongChunk<Values>[][] addChunks = longContext.addChunks;
        final WritableLongChunk<Values>[][] modChunks = longContext.modChunks;

        for (int ri = 0; ri < runs.count; ++ri) {
            final long encoded = runs.encoded[ri];
            final WritableLongChunk<Values>[] originChunks = originChunks(encoded, addChunks, modChunks);

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
     * Implementation of the LongBarrageCopyKernel that delegates to static methods.
     */
    private static class LongBarrageCopyKernelImpl implements BarrageCopyKernel {
        @Override
        public BarrageCopyKernelContext makeContext(WritableChunk<Values>[][] addChunks,
                WritableChunk<Values>[][] modChunks, int deltaChunkSize) {
            return new LongBarrageCopyKernelContext(addChunks, modChunks, deltaChunkSize);
        }

        @Override
        public void copy(Runs runs, WritableChunk<Values>[] dest, BarrageCopyKernelContext context) {
            // noinspection unchecked
            final WritableLongChunk<Values>[] typedDest = new WritableLongChunk[dest.length];
            for (int ii = 0; ii < dest.length; ++ii) {
                typedDest[ii] = dest[ii].asWritableLongChunk();
            }
            LongBarrageCopyKernel.copy(runs, typedDest, context);
        }
    }

    static final BarrageCopyKernel INSTANCE = new LongBarrageCopyKernelImpl();
}
