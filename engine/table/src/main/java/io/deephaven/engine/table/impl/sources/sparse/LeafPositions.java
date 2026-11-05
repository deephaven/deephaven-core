//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.sparse;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK2_SIZE;

/**
 * The positions of the leaves a bottom page table level holds, in dense arrays of its children ordered by leaf index. A
 * leaf index is from 0 to {@link SparseConstants#BLOCK2_SIZE} - 1. The children themselves are kept by the caller, in
 * arrays of their own types, so that reading a child needs no cast; this class only maps a leaf index to its position.
 * <p>
 * Appending changes no position already held, so a reader that finds an index concurrently with an append finds it
 * where it was. Nothing else changes positions in place: the caller makes new positions to add indices among the
 * existing ones or to remove indices.
 */
public abstract class LeafPositions {

    /**
     * @return the number of leaf indices held
     */
    public abstract int size();

    /**
     * @return the highest leaf index held, or -1 if none
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
    public abstract LeafPositions append(int[] indices, int count);

    /**
     * @return the leaf indices held, in ascending order
     */
    public abstract int[] indices();

    /**
     * @return an estimate of the bytes this occupies
     */
    public abstract long estimateSize();

    /**
     * @return positions holding no index
     */
    public static LeafPositions empty() {
        return new Sorted(new int[0], 0);
    }

    /**
     * @return positions holding {@code indices[0, count)}, ascending and distinct, in the smallest representation
     */
    public static LeafPositions of(final int[] indices, final int count) {
        return count <= Sorted.CAPACITY ? new Sorted(indices, count) : new Bitmap(indices, count);
    }

    /**
     * Up to {@link #CAPACITY} leaf indices, sorted, packed four 16-bit lanes to a long: one word for up to four
     * indices, four words for more. A lookup counts the indices less than its target a word at a time, with no branch
     * that depends on the indices, as {@link CharSortedLeaf} does. Unused lanes hold {@link #UNUSED}, which no lookup
     * counts, because no target exceeds it.
     */
    private static final class Sorted extends LeafPositions {
        private static final int CAPACITY = 16;
        private static final int UNUSED = BLOCK2_SIZE - 1;
        private static final long LANE_ONES = 0x0001_0001_0001_0001L;
        private static final long GUARD_BITS = 0x8000_8000_8000_8000L;

        private long[] words;
        private int size;

        private Sorted(final int[] indices, final int count) {
            words = new long[wordsFor(count)];
            Arrays.fill(words, UNUSED * LANE_ONES);
            for (int ii = 0; ii < count; ++ii) {
                setIndexAt(ii, indices[ii]);
            }
            size = count;
        }

        private static int wordsFor(final int count) {
            return count <= 4 ? 1 : CAPACITY / 4;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int lastIndex() {
            return size == 0 ? -1 : indexAt(size - 1);
        }

        private int indexAt(final int position) {
            return (int) (words[position >>> 2] >>> ((position & 3) << 4)) & 0xFFFF;
        }

        private void setIndexAt(final int position, final int index) {
            final int shift = (position & 3) << 4;
            final int wi = position >>> 2;
            words[wi] = (words[wi] & ~(0xFFFFL << shift)) | ((long) index << shift);
        }

        private static int lessThan(final long word, final long target) {
            return Long.bitCount(~((word | GUARD_BITS) - target) & GUARD_BITS);
        }

        @Override
        public int find(final int index) {
            final long[] localWords = words;
            final long target = index * LANE_ONES;
            final int position;
            if (localWords.length == 1) {
                position = lessThan(localWords[0], target);
            } else {
                position = lessThan(localWords[0], target) + lessThan(localWords[1], target)
                        + lessThan(localWords[2], target) + lessThan(localWords[3], target);
            }
            return position < size && indexAt(position) == index ? position : -1;
        }

        @Override
        public LeafPositions append(final int[] indices, final int count) {
            final int newSize = size + count;
            if (newSize > CAPACITY) {
                final int[] all = Arrays.copyOf(indices(), newSize);
                System.arraycopy(indices, 0, all, size, count);
                return new Bitmap(all, newSize);
            }
            if (wordsFor(newSize) > words.length) {
                final int oldLength = words.length;
                words = Arrays.copyOf(words, wordsFor(newSize));
                Arrays.fill(words, oldLength, words.length, UNUSED * LANE_ONES);
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
            return 16 + 16 + (long) words.length * Long.BYTES;
        }
    }

    /**
     * Leaf indices in a bitmap, with the number of indices in the words before each word, so that the position of an
     * index is its rank: the stored count for its word plus the set bits below it in its word, as in
     * {@link CharBitmapLeaf}. Ranks are kept through the last word holding an index; a word past it holds no index, so
     * a lookup there finds its bit clear without reading the rank.
     */
    private static final class Bitmap extends LeafPositions {
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
            final int word = index >>> 6;
            final long wordBits = bits[word];
            final long bit = 1L << index;
            if ((wordBits & bit) == 0) {
                return -1;
            }
            return rankBefore[word] + Long.bitCount(wordBits & (bit - 1));
        }

        @Override
        public LeafPositions append(final int[] indices, final int count) {
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
