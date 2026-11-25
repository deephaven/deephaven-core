//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.sparse;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK1_SIZE;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK2_SIZE;

/**
 * The positions of the children a middle or bottom page table level holds, in dense arrays of its children ordered by
 * child index. A child index is from 0 to {@link SparseConstants#BLOCK2_SIZE} - 1, which is also
 * {@link SparseConstants#BLOCK1_SIZE} - 1. The children themselves are kept by the caller, in arrays of their own
 * types, so that reading a child needs no cast; this class only maps a child index to its position.
 * <p>
 * Appending changes no position already held, so a reader that finds an index concurrently with an append finds it
 * where it was. Nothing else changes positions in place: the caller makes new positions to add indices among the
 * existing ones or to remove indices.
 * <p>
 * {@link #find(int)} stays small enough that the JIT inlines it even at a call site it rarely reaches, as do the
 * lookups of a bitmap and of sorted positions of at most four indices, because a call left in a loop that reads a
 * source keeps the JIT from moving the loads of the page table out of the loop.
 */
public abstract class ChildPositions {

    static {
        // the levels share one representation of their positions
        if (BLOCK1_SIZE != BLOCK2_SIZE) {
            throw new IllegalStateException("BLOCK1_SIZE and BLOCK2_SIZE differ");
        }
    }

    /**
     * @return the number of child indices held
     */
    public abstract int size();

    /**
     * @return the highest child index held, or -1 if none
     */
    public abstract int lastIndex();

    /**
     * @return the position of {@code index}, or -1 if it is not held
     */
    public abstract int find(int index);

    /**
     * Add {@code indices[0, count)}, ascending, each more than {@link #lastIndex()}, at the positions after the
     * existing ones.
     *
     * @return the positions to use: this, or a larger representation holding the same indices and the new ones
     */
    public abstract ChildPositions append(int[] indices, int count);

    /**
     * @return the child indices held, in ascending order
     */
    public abstract int[] indices();

    /**
     * @return an estimate of the bytes this occupies
     */
    public abstract long estimateSize();

    /**
     * @return positions holding no index
     */
    public static ChildPositions empty() {
        return new Sorted(new int[0], 0);
    }

    /**
     * @return positions holding {@code indices[0, count)}, ascending and distinct, in the smallest representation
     */
    public static ChildPositions of(final int[] indices, final int count) {
        return count <= Sorted.CAPACITY ? new Sorted(indices, count) : new Bitmap(indices, count);
    }

    /**
     * Merge {@code added[0, count)}, ascending and none of them held, among {@code held}, ascending.
     *
     * @param held the child indices already held, ascending
     * @param added the child indices to add, ascending, none of them in {@code held}; only the first {@code count} are
     *        read
     * @param count the number of indices of {@code added} to merge
     * @param movedTo receives, for each index of {@code held}, its position among the merged indices; at least
     *        {@code held.length} long
     * @return the merged indices, ascending
     */
    public static int[] merge(final int[] held, final int[] added, final int count, final int[] movedTo) {
        final int[] merged = new int[held.length + count];
        int hi = 0;
        int ai = 0;
        for (int mi = 0; mi < merged.length; ++mi) {
            if (ai == count || (hi < held.length && held[hi] < added[ai])) {
                movedTo[hi] = mi;
                merged[mi] = held[hi++];
            } else {
                merged[mi] = added[ai++];
            }
        }
        return merged;
    }

    /**
     * Find the indices of {@code held}, ascending, that are not among {@code removed[0, count)}, ascending.
     *
     * @param held the child indices held, ascending
     * @param removed the child indices to remove, ascending; only the first {@code count} are read, and an index not in
     *        {@code held} is ignored
     * @param count the number of indices of {@code removed} to remove
     * @param kept receives the indices kept, ascending; at least {@code held.length} long
     * @param keptFrom receives, for each index kept, its position in {@code held}; at least {@code held.length} long
     * @return the number of indices kept
     */
    public static int keep(
            final int[] held,
            final int[] removed,
            final int count,
            final int[] kept,
            final int[] keptFrom) {
        int keptCount = 0;
        int ri = 0;
        for (int hi = 0; hi < held.length; ++hi) {
            while (ri < count && removed[ri] < held[hi]) {
                ++ri;
            }
            if (ri < count && removed[ri] == held[hi]) {
                continue;
            }
            kept[keptCount] = held[hi];
            keptFrom[keptCount++] = hi;
        }
        return keptCount;
    }

    /**
     * Up to {@link #CAPACITY} child indices, sorted, packed four 16-bit lanes to a long in four fields, so that a
     * lookup reads no array. A lookup marks the lanes equal to its target, a word at a time, and takes the position of
     * the lowest mark, with no branch that depends on the indices; a level of at most four children looks only at the
     * first word. Unused lanes hold {@link #UNUSED}, whose top bit no index has, so no lookup marks them. An append in
     * place writes only unused lanes.
     */
    private static final class Sorted extends ChildPositions {
        private static final int CAPACITY = 16;
        private static final int UNUSED = 0xFFFF;
        private static final long LANE_ONES = 0x0001_0001_0001_0001L;
        private static final long GUARD_BITS = 0x8000_8000_8000_8000L;
        private static final long UNUSED_WORD = UNUSED * LANE_ONES;

        private long word0 = UNUSED_WORD;
        private long word1 = UNUSED_WORD;
        private long word2 = UNUSED_WORD;
        private long word3 = UNUSED_WORD;
        private int size;

        private Sorted(final int[] indices, final int count) {
            for (int ii = 0; ii < count; ++ii) {
                setIndexAt(ii, indices[ii]);
            }
            size = count;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int lastIndex() {
            return size == 0 ? -1 : indexAt(size - 1);
        }

        private long word(final int wi) {
            switch (wi) {
                case 0:
                    return word0;
                case 1:
                    return word1;
                case 2:
                    return word2;
                default:
                    return word3;
            }
        }

        private int indexAt(final int position) {
            return (int) (word(position >>> 2) >>> ((position & 3) << 4)) & 0xFFFF;
        }

        private void setIndexAt(final int position, final int index) {
            final int shift = (position & 3) << 4;
            final long lane = (long) index << shift;
            final long mask = ~(0xFFFFL << shift);
            switch (position >>> 2) {
                case 0:
                    word0 = (word0 & mask) | lane;
                    break;
                case 1:
                    word1 = (word1 & mask) | lane;
                    break;
                case 2:
                    word2 = (word2 & mask) | lane;
                    break;
                default:
                    word3 = (word3 & mask) | lane;
                    break;
            }
        }

        /**
         * Mark the lanes of {@code word} equal to their lanes of {@code target}. Subtracting one from each lane of the
         * difference sets the guard bit of a lane that is zero, and borrows from the lane above it, so a lane above an
         * equal lane may be marked too. Below the lowest equal lane nothing borrows, so the marks are exact there: the
         * lowest mark is an equal lane, and there is no mark when no lane is equal. The indices are distinct, so at
         * most one lane is equal, and the lookups need only the lowest mark; a marking exact in every lane would cost
         * more operations for nothing they read.
         *
         * @return the guard bit of each lane of {@code word} equal to its lane of {@code target}, and possibly of lanes
         *         above an equal lane
         */
        private static long equalLanes(final long word, final long target) {
            final long difference = word ^ target;
            return (difference - LANE_ONES) & ~difference & GUARD_BITS;
        }

        @Override
        public int find(final int index) {
            return size <= 4 ? findInFirstWord(index) : findInWords(index);
        }

        /**
         * @return the position of {@code index} among at most four indices, all in the first word, or -1 if not held
         */
        private int findInFirstWord(final int index) {
            final long equal = equalLanes(word0, index * LANE_ONES);
            return equal == 0 ? -1 : Long.numberOfTrailingZeros(equal) >>> 4;
        }

        /**
         * Look up {@code index} in all four words at once. Position {@code p} is lane {@code p & 3} of word
         * {@code p >>> 2}, so a position is {@code 4 * word + lane}.
         * <p>
         * {@link #equalLanes(long, long)} marks lane {@code l} of a word at its guard bit, {@code 16 * l + 15}.
         * Shifting the marks of word {@code w} right by {@code 15 - 4 * w} moves the mark of lane {@code l} to bit
         * {@code 16 * l + 4 * w}:
         *
         * <pre>
         * word  shift  bits of the marks of lanes 0, 1, 2, 3
         * 0     15     0, 16, 32, 48
         * 1     11     4, 20, 36, 52
         * 2      7     8, 24, 40, 56
         * 3      3     12, 28, 44, 60
         * </pre>
         *
         * The words' marks land at different bits modulo 16, so one OR combines them without overlap, and the bit of a
         * mark holds both its lane, {@code bit >>> 4}, and four times its word, {@code bit & 0xC}. Its position,
         * {@code 4 * word + lane}, is therefore {@code (bit & 0xC) | (bit >>> 4)}.
         * <p>
         * The indices are distinct, so at most one word has an equal lane, and the only other marks are of lanes above
         * it in the same word, which land at higher bits. The lowest set bit is therefore the equal lane.
         *
         * @return the position of {@code index}, or -1 if it is not held
         */
        private int findInWords(final int index) {
            final long target = index * LANE_ONES;
            final long equal = equalLanes(word0, target) >>> 15 | equalLanes(word1, target) >>> 11
                    | equalLanes(word2, target) >>> 7 | equalLanes(word3, target) >>> 3;
            if (equal == 0) {
                return -1;
            }
            final int bit = Long.numberOfTrailingZeros(equal);
            return (bit & 0xC) | (bit >>> 4);
        }

        @Override
        public ChildPositions append(final int[] indices, final int count) {
            final int newSize = size + count;
            if (newSize > CAPACITY) {
                final int[] all = new int[newSize];
                System.arraycopy(indices(), 0, all, 0, size);
                System.arraycopy(indices, 0, all, size, count);
                return new Bitmap(all, newSize);
            }
            for (int ii = 0; ii < count; ++ii) {
                setIndexAt(size + ii, indices[ii]);
            }
            size = newSize;
            return this;
        }

        @Override
        public int[] indices() {
            final int[] result = new int[size];
            for (int ii = 0; ii < size; ++ii) {
                result[ii] = indexAt(ii);
            }
            return result;
        }

        @Override
        public long estimateSize() {
            return 16 + 4 * Long.BYTES + Integer.BYTES;
        }
    }

    /**
     * Child indices in a bitmap, with the number of indices in the words before each word, so that the position of an
     * index is its rank: the stored count for its word plus the set bits below it in its word, as in
     * {@link CharBitmapLeaf}. Ranks are kept through the last word holding an index; a word past it holds no index, so
     * a lookup there finds its bit clear without reading the rank.
     */
    private static final class Bitmap extends ChildPositions {
        private static final int WORDS = BLOCK2_SIZE / Long.SIZE;

        private final long[] bits = new long[WORDS];
        private final char[] rankBefore = new char[WORDS];
        private int size;
        /** The last word holding an index, or -1 if none; ranks are kept through it. */
        private int lastWord = -1;

        private Bitmap(final int[] indices, final int count) {
            for (int ii = 0; ii < count; ++ii) {
                bits[indices[ii] >>> 6] |= 1L << indices[ii];
            }
            size = count;
            lastWord = count == 0 ? -1 : indices[count - 1] >>> 6;
            int rank = 0;
            for (int wi = 0; wi <= lastWord; ++wi) {
                rankBefore[wi] = (char) rank;
                rank += Long.bitCount(bits[wi]);
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int lastIndex() {
            return lastWord < 0 ? -1 : (lastWord << 6) + 63 - Long.numberOfLeadingZeros(bits[lastWord]);
        }

        @Override
        public int find(final int index) {
            // shift the bit of index to the sign bit, above the bits of the lower indices in its word
            final long atAndBelow = bits[index >>> 6] << ~index;
            return atAndBelow < 0 ? rank(index, atAndBelow) : -1;
        }

        /**
         * @return the position of {@code index}, which is held, given the bits of its word at and below it
         */
        private int rank(final int index, final long atAndBelow) {
            return rankBefore[index >>> 6] + Long.bitCount(atAndBelow) - 1;
        }

        @Override
        public ChildPositions append(final int[] indices, final int count) {
            for (int ii = 0; ii < count; ++ii) {
                final int index = indices[ii];
                final int word = index >>> 6;
                if (word > lastWord) {
                    // the words from the old last word up to this one hold no index, so their ranks are all the size
                    for (int wi = lastWord + 1; wi <= word; ++wi) {
                        rankBefore[wi] = (char) size;
                    }
                    lastWord = word;
                }
                bits[word] |= 1L << index;
                ++size;
            }
            return this;
        }

        @Override
        public int[] indices() {
            final int[] result = new int[size];
            int next = 0;
            for (int wi = 0; wi <= lastWord; ++wi) {
                long wordBits = bits[wi];
                while (wordBits != 0) {
                    result[next++] = (wi << 6) + Long.numberOfTrailingZeros(wordBits);
                    wordBits &= wordBits - 1;
                }
            }
            return result;
        }

        @Override
        public long estimateSize() {
            return 24 + (16 + (long) WORDS * Long.BYTES) + (16 + (long) WORDS * Character.BYTES);
        }
    }
}
