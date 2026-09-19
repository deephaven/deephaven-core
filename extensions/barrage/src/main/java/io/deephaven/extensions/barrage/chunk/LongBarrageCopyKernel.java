//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageCopyKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

public class LongBarrageCopyKernel {
    /**
     * Context for the LongBarrageCopyKernel that holds the add / mod chunks as WritableLongChunk and the delta chunk
     * size as a shift and a mask.
     */
    private static class LongBarrageCopyKernelContext implements BarrageCopyKernel.BarrageCopyKernelContext {
        private final WritableLongChunk<Values>[][] addChunks;
        private final WritableLongChunk<Values>[][] modChunks;
        private final int deltaChunkSize;
        /**
         * {@code position >>> deltaChunkShift} is the chunk index and {@code position & deltaChunkMask} the offset.
         * Derived here rather than held as constants because the kernel does not own the chunk size: the producer
         * configures it and passes it in, so it is only known once per context, and it must be a power of two.
         */
        private final int deltaChunkShift;
        private final int deltaChunkMask;

        private LongBarrageCopyKernelContext(
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            Assert.assertion(deltaChunkSize > 0 && Integer.bitCount(deltaChunkSize) == 1,
                    "deltaChunkSize is a power of two", deltaChunkSize, "deltaChunkSize");

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
            this.deltaChunkShift = Integer.numberOfTrailingZeros(deltaChunkSize);
            this.deltaChunkMask = deltaChunkSize - 1;
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
     * resulting stretch with one typed array copy.
     */
    private static void copyByRuns(
            final BarrageCopyKernel.Runs runs,
            final WritableLongChunk<Values>[] dest,
            final LongBarrageCopyKernelContext context) {
        // hoisted out of the loops
        final int deltaChunkSize = context.deltaChunkSize;
        final WritableLongChunk<Values>[][] addChunks = context.addChunks;
        final WritableLongChunk<Values>[][] modChunks = context.modChunks;
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
     * Copy every row by first expanding the runs into one encoded origin per output row, chunk by chunk, and then
     * filling the destination in order with the row index as the loop variable. The expansion costs a pass and an array
     * the size of the output, but buys a gather loop with no destination arithmetic and no run bookkeeping, which for
     * short runs is the cheaper trade.
     */
    private static void copyByElements(
            final BarrageCopyKernel.Runs runs,
            final WritableLongChunk<Values>[] dest,
            final LongBarrageCopyKernelContext context) {
        // hoisted out of the loops
        final int shift = context.deltaChunkShift;
        final int mask = context.deltaChunkMask;
        final WritableLongChunk<Values>[][] addChunks = context.addChunks;
        final WritableLongChunk<Values>[][] modChunks = context.modChunks;
        final long[][] mapping = new long[dest.length][];
        for (int mi = 0; mi < dest.length; ++mi) {
            mapping[mi] = new long[dest[mi].size()];
        }
        for (int ri = 0; ri < runs.count; ++ri) {
            long destPos = runs.dest[ri];
            long origin = runs.encoded[ri];
            for (long remaining = runs.len[ri]; remaining > 0; --remaining) {
                mapping[(int) (destPos >>> shift)][(int) (destPos & mask)] = origin;
                ++destPos;
                ++origin;
            }
        }
        for (int mi = 0; mi < dest.length; ++mi) {
            final long[] chunkMapping = mapping[mi];
            final WritableLongChunk<Values> destChunk = dest[mi];
            for (int pos = 0; pos < chunkMapping.length; ++pos) {
                final long encoded = chunkMapping[pos];
                final WritableLongChunk<Values>[] originChunks = originChunks(encoded, addChunks, modChunks);
                final long originPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;
                destChunk.set(pos, originChunks[(int) (originPos >>> shift)].get((int) (originPos & mask)));
            }
        }
    }

    /**
     * Fill the output chunks from the delta chunks according to the runs, choosing once for the whole column between an
     * array copy per stretch and an assignment per element. The runs of one column come from the same updates, so they
     * are alike; deciding per column keeps the decision out of the copy loop.
     */
    private static void copy(
            final BarrageCopyKernel.Runs runs,
            final WritableLongChunk<Values>[] dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {
        if (runs.count == 0) {
            return;
        }

        final LongBarrageCopyKernelContext longContext = (LongBarrageCopyKernelContext) context;
        if (runs.totalRows / runs.count >= BarrageCopyKernel.MIN_AVERAGE_RUN_LENGTH_FOR_ARRAY_COPY) {
            copyByRuns(runs, dest, longContext);
        } else {
            copyByElements(runs, dest, longContext);
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
