//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
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
     * Average run length at or above which a column is copied with an array copy per stretch rather than an element at
     * a time. Benchmarking shows the element path ahead while runs are a row or two long and the array copy ahead from
     * three up, by a margin that widens with the run length. The kernel's average is an integer division, so this is
     * compared against the floor of the true average.
     */
    long MIN_AVERAGE_RUN_LENGTH_FOR_ARRAY_COPY = 3;

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
     * The caller appends the runs in output order and a kernel consumes them, either as runs or by replacing them with
     * the mapping {@link #convertRunsToElementMapping} expands them into. Row sets are range-compressed and updates
     * arrive in ranges, so a run usually covers many rows and building this costs proportionally to ranges rather than
     * rows.
     */
    final class Runs {
        /**
         * Run data for this column: the output positions, encoded origins, and lengths of each contiguous stretch. This
         * may be used directly by the copy kernel or converted to an element mapping for element-wise copying (which
         * will nullify these arrays).
         */
        long[] dest = new long[16];
        long[] encoded = new long[16];
        long[] len = new long[16];
        int count;
        /** Sum of the run lengths, so a kernel can size its strategy without a pass over the runs. */
        long totalRows;
        /**
         * Once decided to use element-wise copying, this holds the mapping from output rows to encoded origins. It is
         * null until {@link #convertRunsToElementMapping} is called.
         */
        long[][] elementMapping;

        /** How many output rows these runs account for between them. */
        public long totalRows() {
            return totalRows;
        }

        /**
         * Append one run: {@code length} rows starting at output position {@code destination}, whose first row is at
         * encoded origin {@code origin} and whose remaining rows follow it contiguously.
         */
        public void add(final long destination, final long origin, final long length) {
            Assert.eqNull(elementMapping, "elementMapping");
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


        /**
         * Replace the runs with {@link #elementMapping}, one encoded origin per output row, in output order, laid out
         * in the output's chunks. Idempotent — once the mapping is there, this does nothing.
         *
         * <p>
         * The mapping depends only on the runs and the chunk size, never on a column's data, and every column whose
         * rows come from the same deltas in the same pattern shares one {@code Runs}, so the columns of one coalesce
         * that need it build it once between them rather than each allocating eight bytes per row. Coalescing is single
         * threaded, which is what makes the unsynchronized caching safe.
         *
         * <p>
         * The runs are dropped once converted, because they are the larger of the two: three longs per run against one
         * per row, so at a run per row they cost three times the mapping, more once {@link #add}'s doubling has left
         * slack. Nothing reads them afterwards: a kernel copies by run only while {@link #elementMapping} is null, so
         * the first column to convert commits the rest to the mapping.
         *
         * @param deltaChunkSize rows per output chunk except the last; must be a power of two
         */
        void convertRunsToElementMapping(final int deltaChunkSize) {
            if (elementMapping != null) {
                return;
            }
            final int numChunks = (int) ((totalRows + deltaChunkSize - 1) / deltaChunkSize);
            final long[][] mapping = new long[numChunks][];
            for (int mi = 0; mi < numChunks; ++mi) {
                final int rows = (mi < numChunks - 1 || totalRows % deltaChunkSize == 0)
                        ? deltaChunkSize
                        : (int) (totalRows % deltaChunkSize);
                mapping[mi] = new long[rows];
            }
            final int shift = Integer.numberOfTrailingZeros(deltaChunkSize);
            final int mask = deltaChunkSize - 1;
            for (int ri = 0; ri < count; ++ri) {
                long destPos = dest[ri];
                long origin = encoded[ri];
                for (long remaining = len[ri]; remaining > 0; --remaining) {
                    mapping[(int) (destPos >>> shift)][(int) (destPos & mask)] = origin;
                    ++destPos;
                    ++origin;
                }
            }
            dest = null;
            encoded = null;
            len = null;
            elementMapping = mapping;
        }
    }

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
     * Base context for a BarrageCopyKernel.
     **/
    interface BarrageCopyKernelContext {
        int deltaChunkSize();
    }

    /**
     * Create a context for this copy kernel that will contain add / mod delta chunks cast to the correct type.
     *
     * @param addChunks the add delta chunks (per delta)
     * @param modChunks the mod delta chunks (per delta)
     * @param deltaChunkSize the number of rows in every delta chunk except the last of a column, which is what makes an
     *        encoded position locate a chunk by a shift; must be a power of two
     * @return a context that can be passed to {@link #copy(Runs, WritableChunk[], BarrageCopyKernelContext)} to drive
     *         the copy from the add / mod delta chunks into the output chunks.
     */
    BarrageCopyKernelContext makeContext(
            WritableChunk<Values>[][] addChunks,
            WritableChunk<Values>[][] modChunks,
            int deltaChunkSize);

    /**
     * Fill one column's output chunks from the per-delta chunks the context holds, following {@code runs}.
     *
     * <p>
     * Chooses once per column, from the average run length against {@link #MIN_AVERAGE_RUN_LENGTH_FOR_ARRAY_COPY},
     * between an array copy per run and an element at a time; a column whose runs another column has already converted
     * takes the element copy whatever its run length.
     *
     * @param runs where every output row comes from, in output order
     * @param dest the output chunks to fill, all of {@link BarrageCopyKernelContext#deltaChunkSize()} rows except the
     *        last, holding {@code runs.totalRows} rows between them
     * @param context the context returned from {@link #makeContext(WritableChunk[][], WritableChunk[][], int)}
     */
    void copy(
            Runs runs,
            WritableChunk<Values>[] dest,
            BarrageCopyKernelContext context);
}
