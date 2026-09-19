//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;

public class ObjectBarrageCopyKernel {
    /**
     * Context for the ObjectBarrageCopyKernel that holds the add / mod chunks as WritableObjectChunk and the delta
     * chunk size as a shift and a mask.
     */
    private static class ObjectBarrageCopyKernelContext implements BarrageCopyKernel.BarrageCopyKernelContext {
        private final WritableObjectChunk<Object, Values>[][] addChunks;
        private final WritableObjectChunk<Object, Values>[][] modChunks;
        private final int deltaChunkSize;
        /**
         * {@code position >>> deltaChunkShift} is the chunk index and {@code position & deltaChunkMask} the offset.
         * Derived here rather than held as constants because the kernel does not own the chunk size: the producer
         * configures it and passes it in, so it is only known once per context, and it must be a power of two.
         */
        private final int deltaChunkShift;
        private final int deltaChunkMask;

        private ObjectBarrageCopyKernelContext(
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            Assert.assertion(deltaChunkSize > 0 && Integer.bitCount(deltaChunkSize) == 1,
                    "deltaChunkSize is a power of two", deltaChunkSize, "deltaChunkSize");

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
     * Copy every run, splitting a run wherever it crosses an origin or destination chunk boundary and moving each
     * resulting stretch with one typed array copy.
     */
    private static void copyByRuns(
            final BarrageCopyKernel.Runs runs,
            final WritableObjectChunk<Object, Values>[] dest,
            final ObjectBarrageCopyKernelContext context) {
        // hoisted out of the loops
        final int deltaChunkSize = context.deltaChunkSize;
        final WritableObjectChunk<Object, Values>[][] addChunks = context.addChunks;
        final WritableObjectChunk<Object, Values>[][] modChunks = context.modChunks;
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
     * Copy every run by assigning one element at a time, addressed straight from the runs: with a power-of-two chunk
     * size, locating an element's chunk and offset is a shift and a mask, so a run that crosses a chunk boundary needs
     * no special handling. The primitive kernels instead expand the runs into a per-row mapping first, which is faster
     * for them; for references the mapping's allocation, landing in a heap full of the very objects being copied,
     * measured twice as slow as this, so this kernel does not build one.
     */
    private static void copyByElements(
            final BarrageCopyKernel.Runs runs,
            final WritableObjectChunk<Object, Values>[] dest,
            final ObjectBarrageCopyKernelContext context) {
        // hoisted out of the loops
        final int shift = context.deltaChunkShift;
        final int mask = context.deltaChunkMask;
        final WritableObjectChunk<Object, Values>[][] addChunks = context.addChunks;
        final WritableObjectChunk<Object, Values>[][] modChunks = context.modChunks;
        for (int ri = 0; ri < runs.count; ++ri) {
            final long encoded = runs.encoded[ri];
            final WritableObjectChunk<Object, Values>[] originChunks = originChunks(encoded, addChunks, modChunks);
            final long originStart = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;
            final long destStart = runs.dest[ri];
            final long length = runs.len[ri];
            for (long ii = 0; ii < length; ++ii) {
                final long originPos = originStart + ii;
                final long destPos = destStart + ii;
                dest[(int) (destPos >>> shift)].set((int) (destPos & mask),
                        originChunks[(int) (originPos >>> shift)].get((int) (originPos & mask)));
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
            final WritableObjectChunk<Object, Values>[] dest,
            final BarrageCopyKernel.BarrageCopyKernelContext context) {
        if (runs.count == 0) {
            return;
        }

        final ObjectBarrageCopyKernelContext objectContext = (ObjectBarrageCopyKernelContext) context;
        if (runs.totalRows / runs.count >= BarrageCopyKernel.MIN_AVERAGE_RUN_LENGTH_FOR_ARRAY_COPY) {
            copyByRuns(runs, dest, objectContext);
        } else {
            copyByElements(runs, dest, objectContext);
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
