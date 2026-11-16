//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSortedLeaf and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.INDEX_MASK;
import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * A {@link IntCompactLeaf} for a few values: the offsets that hold a value in ascending order, and their values at the
 * same positions. It has no per-leaf structure beyond those two arrays.
 * <p>
 * The offsets are packed four to a long, in 16 bit lanes from the lowest, and lanes past {@link #size} hold
 * {@link #PAST_BLOCK}. A lookup counts the offsets less than its target a word at a time, with no branch that depends
 * on the offsets.
 */
public final class IntSortedLeaf extends IntCompactLeaf {
    /**
     * An offset past the end of any block, for unused lanes.
     */
    private static final int PAST_BLOCK = Short.MAX_VALUE;
    private static final long LANE_ONES = 0x0001_0001_0001_0001L;
    private static final long GUARD_BITS = 0x8000_8000_8000_8000L;
    private static final long PAST_BLOCK_LANES = PAST_BLOCK * LANE_ONES;

    /** A leaf holding no values. */
    public static final IntSortedLeaf EMPTY = new IntSortedLeaf(newWords(0), new int[0]);

    private final long[] words;
    private final int[] values;
    private final int size;

    /**
     * Make a leaf of the offsets in {@code words} and their values. The arrays are complete before the leaf is made, so
     * that a reader on another thread that finds the leaf sees them in full.
     *
     * @param words the offsets, packed as {@link #newWords(int)} lays them out
     * @param values the value of each offset, one for each offset
     */
    private IntSortedLeaf(final long[] words, final int[] values) {
        this.words = words;
        this.values = values;
        size = values.length;
    }

    /**
     * @return the words for {@code size} offsets, every lane holding {@link #PAST_BLOCK}
     */
    private static long[] newWords(final int size) {
        final long[] words = new long[wordsFor(size)];
        Arrays.fill(words, PAST_BLOCK_LANES);
        return words;
    }

    /**
     * @return the words for {@code capacity} offsets: one word for up to four, otherwise at least four words, so that
     *         most leaves count a fixed number of words
     */
    private static int wordsFor(final int capacity) {
        return capacity <= 4 ? 1 : Math.max(4, (capacity + 3) >>> 2);
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * @return the offset at {@code index}, which is less than {@link #size}
     */
    int offsetAt(final int index) {
        return (int) (words[index >>> 2] >>> ((index & 3) << 4)) & 0xFFFF;
    }

    /**
     * @return the value at {@code index}, which is less than {@link #size}
     */
    int valueAt(final int index) {
        return values[index];
    }

    private static void setOffset(final long[] words, final int index, final int offset) {
        final int shift = (index & 3) << 4;
        final int wi = index >>> 2;
        words[wi] = (words[wi] & ~(0xFFFFL << shift)) | ((long) offset << shift);
    }

    /**
     * @return the number of offsets less than {@code offset}
     */
    private int lowerBound(final int offset) {
        final long[] words = this.words;
        final long target = offset * LANE_ONES;
        // the common lengths are counted without a loop: C2 vectorizes a loop over the words, and its setup costs more
        // than counting four words
        if (words.length == 4) {
            return lessThan(words[0], target) + lessThan(words[1], target) + lessThan(words[2], target)
                    + lessThan(words[3], target);
        }
        if (words.length == 1) {
            return lessThan(words[0], target);
        }
        int count = 0;
        for (int wi = 0; wi < words.length; ++wi) {
            count += lessThan(words[wi], target);
        }
        return count;
    }

    /**
     * @return the number of lanes of {@code word} less than the lanes of {@code target}, which all hold one offset
     */
    private static int lessThan(final long word, final long target) {
        // setting the guard bit of each lane makes it at least 0x8000, more than any target, so subtracting the target
        // borrows from no other lane, and leaves the guard bit set exactly when the lane is at least the target
        return Long.bitCount(~((word | GUARD_BITS) - target) & GUARD_BITS);
    }

    /**
     * @return the index of {@code offset}, or {@code -(insertion index) - 1} if it holds no value
     */
    private int indexOf(final int offset) {
        final int index = lowerBound(offset);
        return index < size && offsetAt(index) == offset ? index : -index - 1;
    }

    @Override
    public boolean contains(final int offset) {
        return indexOf(offset) >= 0;
    }

    @Override
    public int get(final int offset) {
        final int index = indexOf(offset);
        return index < 0 ? NULL_INT : values[index];
    }

    @Override
    public IntCompactLeaf put(final int offset, final int value, final int sortedThreshold) {
        int index = indexOf(offset);
        if (index >= 0) {
            values[index] = value;
            return this;
        }
        index = -index - 1;
        if (size + 1 > sortedThreshold) {
            return new IntBitmapLeaf(this, offset, value, index);
        }
        final int[] newValues = new int[size + 1];
        System.arraycopy(values, 0, newValues, 0, index);
        newValues[index] = value;
        System.arraycopy(values, index, newValues, index + 1, size - index);
        final long[] newWords = newWords(size + 1);
        for (int ii = 0; ii < index; ++ii) {
            setOffset(newWords, ii, offsetAt(ii));
        }
        setOffset(newWords, index, offset);
        for (int ii = index; ii < size; ++ii) {
            setOffset(newWords, ii + 1, offsetAt(ii));
        }
        return new IntSortedLeaf(newWords, newValues);
    }

    @Override
    public int countAbsent(final int[] offsets, final int count) {
        // both lists ascend, so one pass over each finds the offsets this leaf does not hold
        int absent = 0;
        int index = 0;
        for (int ii = 0; ii < count; ++ii) {
            final int offset = offsets[ii];
            while (index < size && offsetAt(index) < offset) {
                ++index;
            }
            if (index < size && offsetAt(index) == offset) {
                ++index;
            } else {
                ++absent;
            }
        }
        return absent;
    }

    @Override
    public IntCompactLeaf putSorted(final int[] offsets, final IntChunk<?> src, final int srcOffset,
            final int count, final int sortedThreshold) {
        if (count == 1) {
            return put(offsets[0], src.get(srcOffset), sortedThreshold);
        }
        final int newSize = size + countAbsent(offsets, count);
        if (newSize == size) {
            // both lists ascend, so one pass finds the index of each offset
            int index = 0;
            for (int ii = 0; ii < count; ++ii) {
                final int offset = offsets[ii];
                while (offsetAt(index) < offset) {
                    ++index;
                }
                values[index++] = src.get(srcOffset + ii);
            }
            return this;
        }
        return merge(offsets, src, srcOffset, count, newSize, sortedThreshold);
    }

    @Override
    public IntCompactLeaf insertAbsent(final int[] offsets, final int count, final int sortedThreshold) {
        final int newSize = size + countAbsent(offsets, count);
        return newSize == size ? this : merge(offsets, null, 0, count, newSize, sortedThreshold);
    }

    /**
     * @return a new leaf of {@code newSize} values holding this leaf's offsets and {@code offsets[0, count)}, ascending
     *         and distinct, with the values of {@code src} from {@code srcOffset} for those offsets, or, if {@code src}
     *         is null, keeping the values of the offsets this leaf holds and null for the others; a bitmap leaf if
     *         {@code newSize} is more than {@code sortedThreshold}, else a sorted leaf
     */
    private IntCompactLeaf merge(
            final int[] offsets,
            final IntChunk<?> src,
            final int srcOffset,
            final int count,
            final int newSize,
            final int sortedThreshold) {
        if (newSize > sortedThreshold) {
            return new IntBitmapLeaf(this, offsets, src, srcOffset, count, newSize);
        }
        final long[] newWords = newWords(newSize);
        final int[] newValues = new int[newSize];
        int index = 0;
        int dest = 0;
        for (int ii = 0; ii < count; ++ii) {
            final int offset = offsets[ii];
            int existing;
            while (index < size && (existing = offsetAt(index)) < offset) {
                setOffset(newWords, dest, existing);
                newValues[dest++] = values[index++];
            }
            final boolean present = index < size && offsetAt(index) == offset;
            setOffset(newWords, dest, offset);
            if (src != null) {
                newValues[dest++] = src.get(srcOffset + ii);
            } else {
                newValues[dest++] = present ? values[index] : NULL_INT;
            }
            if (present) {
                ++index;
            }
        }
        while (index < size) {
            setOffset(newWords, dest, offsetAt(index));
            newValues[dest++] = values[index++];
        }
        return new IntSortedLeaf(newWords, newValues);
    }

    @Override
    public IntCompactLeaf remove(final int offset) {
        final int index = indexOf(offset);
        if (index < 0) {
            return this;
        }
        return without(index, index + 1);
    }

    @Override
    public IntCompactLeaf removeRange(final int firstOffset, final int lastOffset) {
        final int start = lowerBound(firstOffset);
        final int end = lowerBound(lastOffset + 1);
        return start == end ? this : without(start, end);
    }

    /**
     * @return a new leaf holding this leaf's offsets and values except those at indices {@code [start, end)}
     */
    private IntSortedLeaf without(final int start, final int end) {
        final int newSize = size - (end - start);
        final long[] newWords = newWords(newSize);
        final int[] newValues = new int[newSize];
        for (int ii = 0; ii < start; ++ii) {
            setOffset(newWords, ii, offsetAt(ii));
        }
        for (int ii = end; ii < size; ++ii) {
            setOffset(newWords, ii - (end - start), offsetAt(ii));
        }
        System.arraycopy(values, 0, newValues, 0, start);
        System.arraycopy(values, end, newValues, start, size - end);
        return new IntSortedLeaf(newWords, newValues);
    }

    @Override
    public void copyTo(final int[] block) {
        for (int ii = 0; ii < size; ++ii) {
            block[offsetAt(ii)] = values[ii];
        }
    }

    @Override
    public int copyOffsets(final int firstOffset, final int lastOffset, final int[] dest) {
        int count = 0;
        for (int index = lowerBound(firstOffset); index < size; ++index) {
            final int offset = offsetAt(index);
            if (offset > lastOffset) {
                break;
            }
            dest[count++] = offset;
        }
        return count;
    }

    @Override
    public void overlayRange(
            final int firstOffset,
            final int lastOffset,
            final WritableIntChunk<?> dest,
            final int destOffset) {
        for (int index = lowerBound(firstOffset); index < size; ++index) {
            final int offset = offsetAt(index);
            if (offset > lastOffset) {
                break;
            }
            dest.set(destOffset + offset - firstOffset, values[index]);
        }
    }

    @Override
    public void fillRange(
            final int firstOffset,
            final int lastOffset,
            final WritableIntChunk<?> dest,
            final int destOffset) {
        int index = lowerBound(firstOffset);
        for (int offset = firstOffset, di = destOffset; offset <= lastOffset; ++offset, ++di) {
            if (index < size && offsetAt(index) == offset) {
                dest.set(di, values[index++]);
            } else {
                dest.set(di, NULL_INT);
            }
        }
    }

    @Override
    public void fillKeys(
            final LongChunk<? extends RowKeys> keys,
            final int start,
            final int end,
            final WritableIntChunk<?> dest,
            final int destOffset) {
        // the offsets ascend, so the index of each is at or after the index of the one before
        int index = lowerBound((int) (keys.get(start) & INDEX_MASK));
        for (int ii = start, di = destOffset; ii < end; ++ii, ++di) {
            final int offset = (int) (keys.get(ii) & INDEX_MASK);
            while (index < size && offsetAt(index) < offset) {
                ++index;
            }
            dest.set(di, index < size && offsetAt(index) == offset ? values[index] : NULL_INT);
        }
    }

    @Override
    public void overlayKeys(
            final LongChunk<? extends RowKeys> keys,
            final int start,
            final int end,
            final WritableIntChunk<?> dest,
            final int destOffset) {
        int index = lowerBound((int) (keys.get(start) & INDEX_MASK));
        for (int ii = start, di = destOffset; ii < end && index < size; ++ii, ++di) {
            final int offset = (int) (keys.get(ii) & INDEX_MASK);
            while (index < size && offsetAt(index) < offset) {
                ++index;
            }
            if (index < size && offsetAt(index) == offset) {
                dest.set(di, values[index]);
            }
        }
    }

    @Override
    public long estimateSize() {
        // object header and fields, then the offsets and values, each array with a 16 byte header
        return 24 + (16 + (long) words.length * Long.BYTES) + (16 + (long) values.length * Integer.BYTES);
    }
}
