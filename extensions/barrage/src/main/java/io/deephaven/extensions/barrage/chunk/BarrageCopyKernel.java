//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

import java.util.Arrays;

/**
 * Typed kernel for gathering one column of a coalesced Barrage update out of the chunks of the per-cycle updates it was
 * built from. The caller describes where every output row comes from as {@link Runs}; this kernel owns how the data
 * moves.
 *
 * <p>
 * Replicated per chunk type only so the loop's copy call stays monomorphic. One shared implementation would see every
 * chunk class, go megamorphic, and stop the typed copy inlining: measured a third slower at short run lengths.
 */
public interface BarrageCopyKernel {

    // We use bit encoding for delta chunk origins to avoid binary search or iteration to determine source
    // chunks. The following are the bit mapping for the values (stored as long).
    // bits 0-39 = position within that delta's chunk list (up to 1,099,511,627,776 unique positions).
    long DELTA_POSITION_MASK = 0xFFFFFFFFFFL;
    // bits 40-61 = actual delta index into pendingDeltas (up to 4,194,304 unique deltas per update)
    int DELTA_INDEX_SHIFT = 40;
    long DELTA_INDEX_MASK = 0x3FFFFFL;
    // bit 62 = 0 for addChunks or 1 for modChunks
    int DELTA_MOD_FLAG_BIT = 62;

    /**
     * Position zero of one side (adds or mods) of one delta, in the bit layout above. Add a position within that side's
     * chunks, or shift a whole row set of positions by it, for the encoded origins {@link Runs} carries.
     */
    static long originOffset(final int deltaIndex, final boolean fromModChunks) {
        final long base = ((long) deltaIndex) << DELTA_INDEX_SHIFT;
        return fromModChunks ? base | (1L << DELTA_MOD_FLAG_BIT) : base;
    }

    /**
     * Where every row of one output column comes from, as stretches that are contiguous in both the output and the
     * delta chunk they come from: for each, the first output position, the encoded origin of its first row (see
     * {@link #originOffset}) and how many rows follow contiguously.
     *
     * <p>
     * No column data lives here, only the positions that say where to find it. The caller appends the runs in output
     * order and a kernel reads them; nothing consumes or rewrites them, which is what lets columns whose rows come from
     * the same places share one. Row sets are range-compressed and updates arrive in ranges, so a run usually covers
     * many rows and building this costs proportionally to ranges rather than rows.
     */
    final class Runs {
        /** The output positions, encoded origins and lengths of each contiguous stretch. */
        long[] dest = new long[16];
        long[] encoded = new long[16];
        long[] len = new long[16];
        int count;
        /** Sum of the run lengths, so the caller can size the output without a pass over the runs. */
        long totalRows;

        /** How many output rows these runs account for between them. */
        public long totalRows() {
            return totalRows;
        }

        /**
         * Append one run: {@code length} rows starting at output position {@code destination}, whose first row is at
         * encoded origin {@code origin} and whose remaining rows follow it contiguously.
         */
        public void add(final long destination, final long origin, final long length) {
            if (count == dest.length) {
                dest = Arrays.copyOf(dest, count * 2);
                encoded = Arrays.copyOf(encoded, count * 2);
                len = Arrays.copyOf(len, count * 2);
            }
            dest[count] = destination;
            encoded[count] = origin;
            len[count] = length;
            ++count;
            totalRows += length;
        }
    }

    /**
     * The kernel for {@code chunkType}: a stateless shared instance, not a new one, named to match
     * {@link io.deephaven.engine.table.impl.util.copy.CopyKernel#makeCopyKernel}.
     */
    static BarrageCopyKernel makeBarrageCopyKernel(final ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return CharBarrageCopyKernel.INSTANCE;
            case Byte:
                return ByteBarrageCopyKernel.INSTANCE;
            case Short:
                return ShortBarrageCopyKernel.INSTANCE;
            case Int:
                return IntBarrageCopyKernel.INSTANCE;
            case Long:
                return LongBarrageCopyKernel.INSTANCE;
            case Float:
                return FloatBarrageCopyKernel.INSTANCE;
            case Double:
                return DoubleBarrageCopyKernel.INSTANCE;
            default:
                return ObjectBarrageCopyKernel.INSTANCE;
        }
    }

    /**
     * Fill one column's output chunks from one column's per-delta chunks, following {@code runs}.
     *
     * <p>
     * Each run is split wherever it crosses an origin or destination chunk boundary, and every resulting stretch moves
     * with one typed array copy. An implementation casts the chunk arrays to its own chunk type once per call, so there
     * is nothing worth keeping between calls and nothing to close afterwards.
     *
     * @param runs where every output row comes from, in output order
     * @param dest the output chunks to fill, all of {@code deltaChunkSize} rows except the last, holding
     *        {@code runs.totalRows} rows between them
     * @param addChunks the add delta chunks for this column, indexed by the delta index a run's origin encodes
     * @param modChunks the mod delta chunks for this column, indexed the same way
     * @param deltaChunkSize the number of rows in every delta chunk except the last of a column, which is what lets an
     *        encoded position locate its chunk. Must be a power of two.
     */
    void copy(
            Runs runs,
            WritableChunk<Values>[] dest,
            WritableChunk<Values>[][] addChunks,
            WritableChunk<Values>[][] modChunks,
            int deltaChunkSize);
}
