//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageCopyKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

public class ObjectBarrageCopyKernel {
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
     * One side's chunks as the concrete chunk type, so that the copy loop makes no cast of its own and its copy call
     * stays monomorphic. A null entry is a delta that recorded nothing on this side for this column, which the runs
     * then never name.
     */
    private static WritableObjectChunk<Object, Values>[][] asTypedChunks(final WritableChunk<Values>[][] chunks) {
        // noinspection unchecked
        final WritableObjectChunk<Object, Values>[][] typed = new WritableObjectChunk[chunks.length][];
        for (int di = 0; di < chunks.length; ++di) {
            if (chunks[di] == null) {
                continue;
            }
            // noinspection unchecked
            typed[di] = new WritableObjectChunk[chunks[di].length];
            for (int ci = 0; ci < chunks[di].length; ++ci) {
                typed[di][ci] = chunks[di][ci].asWritableObjectChunk();
            }
        }
        return typed;
    }

    /**
     * Implementation of the ObjectBarrageCopyKernel that delegates to static methods.
     */
    private static class ObjectBarrageCopyKernelImpl implements BarrageCopyKernel {
        @Override
        public void copy(
                final Runs runs,
                final WritableChunk<Values>[] dest,
                final WritableChunk<Values>[][] addChunks,
                final WritableChunk<Values>[][] modChunks,
                final int deltaChunkSize) {
            Assert.eqTrue(Integer.bitCount(deltaChunkSize) == 1, "deltaChunkSize is a power of two");

            // Cast every chunk once, here, rather than per run: the loop below then sees only the concrete chunk type
            // and its copy inlines.
            // noinspection unchecked
            final WritableObjectChunk<Object, Values>[] typedDest = new WritableObjectChunk[dest.length];
            for (int ii = 0; ii < dest.length; ++ii) {
                typedDest[ii] = dest[ii].asWritableObjectChunk();
            }
            final WritableObjectChunk<Object, Values>[][] typedAddChunks = asTypedChunks(addChunks);
            final WritableObjectChunk<Object, Values>[][] typedModChunks = asTypedChunks(modChunks);

            // Every chunk holds deltaChunkSize rows but the last of a column, so a position's chunk and its offset
            // within that chunk are a shift and a mask.
            final int chunkShift = Integer.numberOfTrailingZeros(deltaChunkSize);
            final long offsetMask = deltaChunkSize - 1;

            for (int ri = 0; ri < runs.count; ++ri) {
                final long encoded = runs.encoded[ri];
                final WritableObjectChunk<Object, Values>[] originChunks =
                        originChunks(encoded, typedAddChunks, typedModChunks);

                long originPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;
                long destPos = runs.dest[ri];
                long remaining = runs.len[ri];
                while (remaining > 0) {
                    final int originOff = (int) (originPos & offsetMask);
                    final int destOff = (int) (destPos & offsetMask);
                    final int length = (int) Math.min(remaining,
                            Math.min(deltaChunkSize - originOff, deltaChunkSize - destOff));
                    typedDest[(int) (destPos >>> chunkShift)].copyFromTypedChunk(
                            originChunks[(int) (originPos >>> chunkShift)], originOff, destOff, length);
                    originPos += length;
                    destPos += length;
                    remaining -= length;
                }
            }
        }
    }

    static final BarrageCopyKernel INSTANCE = new ObjectBarrageCopyKernelImpl();
}
