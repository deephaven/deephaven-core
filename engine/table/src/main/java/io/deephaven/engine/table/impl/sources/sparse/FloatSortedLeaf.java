//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSortedLeaf and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.WritableFloatChunk;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * A {@link FloatCompactLeaf} for a few values: the offsets that hold a value in ascending order, and their values at the
 * same positions. It has no per-leaf structure beyond those two arrays, which grow as values are added.
 * <p>
 * The offsets are packed four to a long, in 16 bit lanes from the lowest, and lanes past {@link #size} hold
 * {@link #PAST_BLOCK}. A lookup counts the offsets less than its target a word at a time, with no branch that depends
 * on the offsets.
 */
public final class FloatSortedLeaf extends FloatCompactLeaf {
    /**
     * An offset past the end of any block, for unused lanes.
     */
    private static final int PAST_BLOCK = Short.MAX_VALUE;
    private static final long LANE_ONES = 0x0001_0001_0001_0001L;
    private static final long GUARD_BITS = 0x8000_8000_8000_8000L;
    private static final long PAST_BLOCK_LANES = PAST_BLOCK * LANE_ONES;

    private long[] words;
    private float[] values;
    private int size;

    /**
     * @param initialCapacity the number of values to allocate room for
     */
    public FloatSortedLeaf(final int initialCapacity) {
        words = new long[wordsFor(initialCapacity)];
        Arrays.fill(words, PAST_BLOCK_LANES);
        values = new float[Math.max(1, initialCapacity)];
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
    private int offsetAt(final int index) {
        return (int) (words[index >>> 2] >>> ((index & 3) << 4)) & 0xFFFF;
    }

    private void setOffset(final int index, final int offset) {
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

    /**
     * Make room for at least {@code capacity} offsets and values.
     */
    private void ensureCapacity(final int capacity) {
        if (capacity > words.length * 4) {
            final int oldLength = words.length;
            words = Arrays.copyOf(words, wordsFor(Math.max(capacity, size * 2)));
            Arrays.fill(words, oldLength, words.length, PAST_BLOCK_LANES);
        }
        if (capacity > values.length) {
            values = Arrays.copyOf(values, Math.max(capacity, size * 2));
        }
    }

    @Override
    public boolean contains(final int offset) {
        return indexOf(offset) >= 0;
    }

    @Override
    public float get(final int offset) {
        final int index = indexOf(offset);
        return index < 0 ? NULL_FLOAT : values[index];
    }

    @Override
    public void put(final int offset, final float value) {
        int index = indexOf(offset);
        if (index >= 0) {
            values[index] = value;
            return;
        }
        index = -index - 1;
        ensureCapacity(size + 1);
        for (int ii = size; ii > index; --ii) {
            setOffset(ii, offsetAt(ii - 1));
        }
        setOffset(index, offset);
        if (index < size) {
            System.arraycopy(values, index, values, index + 1, size - index);
        }
        values[index] = value;
        ++size;
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
    public void putSorted(final int[] offsets, final FloatChunk<?> src, final int srcOffset, final int count) {
        if (count == 1) {
            put(offsets[0], src.get(srcOffset));
            return;
        }
        final int newSize = size + countAbsent(offsets, count);
        ensureCapacity(newSize);
        // merge from the highest offset down, so that each entry moves at most once and lands above any entry not yet
        // moved
        int index = size - 1;
        int dest = newSize - 1;
        for (int ii = count - 1; ii >= 0;) {
            final int offset = offsets[ii];
            final int existing = index >= 0 ? offsetAt(index) : -1;
            if (existing > offset) {
                setOffset(dest, existing);
                values[dest] = values[index--];
            } else {
                if (existing == offset) {
                    --index;
                }
                setOffset(dest, offset);
                values[dest] = src.get(srcOffset + ii--);
            }
            --dest;
        }
        size = newSize;
    }

    @Override
    public void remove(final int offset) {
        final int index = indexOf(offset);
        if (index < 0) {
            return;
        }
        for (int ii = index; ii < size - 1; ++ii) {
            setOffset(ii, offsetAt(ii + 1));
        }
        setOffset(size - 1, PAST_BLOCK);
        System.arraycopy(values, index + 1, values, index, size - index - 1);
        values[--size] = NULL_FLOAT;
    }

    @Override
    public void copyTo(final float[] block) {
        for (int ii = 0; ii < size; ++ii) {
            block[offsetAt(ii)] = values[ii];
        }
    }

    /**
     * Pass every offset and value to {@code destination}, in offset order.
     */
    public void copyTo(final FloatCompactLeaf destination) {
        for (int ii = 0; ii < size; ++ii) {
            destination.put(offsetAt(ii), values[ii]);
        }
    }

    @Override
    public void fillRange(
            final int firstOffset,
            final int lastOffset,
            final WritableFloatChunk<?> dest,
            final int destOffset) {
        int index = lowerBound(firstOffset);
        for (int offset = firstOffset, di = destOffset; offset <= lastOffset; ++offset, ++di) {
            if (index < size && offsetAt(index) == offset) {
                dest.set(di, values[index++]);
            } else {
                dest.set(di, NULL_FLOAT);
            }
        }
    }

    @Override
    public long estimateSize() {
        // object header and fields, then the offsets and values, each array with a 16 byte header
        return 24 + (16 + (long) words.length * Long.BYTES) + (16 + (long) values.length * Float.BYTES);
    }
}
