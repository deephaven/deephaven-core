//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBitmapLeaf and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableShortChunk;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * A {@link ShortCompactLeaf} that records which offsets hold a value in a bitmap and stores the values densely in offset
 * order. The value for an offset is at its rank: the number of set bits before it, which is the stored rank at the
 * start of the offset's bitmap word plus the set bits before the offset within that word.
 */
public final class ShortBitmapLeaf extends ShortCompactLeaf {
    private static final int WORDS = BLOCK_SIZE / Long.SIZE;

    /** Bit {@code offset % 64} of word {@code offset / 64} is set when {@code offset} holds a value. */
    private final long[] bits = new long[WORDS];
    /** The number of values in the words before each word. */
    private final short[] rankBefore = new short[WORDS];
    /** The values, in offset order; only the first {@link #size} entries are used. */
    private short[] values;
    private int size;

    /**
     * @param initialCapacity the number of values to allocate room for
     */
    public ShortBitmapLeaf(final int initialCapacity) {
        values = new short[Math.max(1, initialCapacity)];
    }

    /**
     * @return the number of values at offsets before {@code offset}
     */
    private int rank(final int offset) {
        final int word = offset >>> 6;
        return rankBefore[word] + Long.bitCount(bits[word] & ((1L << offset) - 1));
    }

    private void adjustRanksAfter(final int word, final int delta) {
        for (int wi = word + 1; wi < WORDS; ++wi) {
            rankBefore[wi] += delta;
        }
    }

    private void ensureCapacity(final int capacity) {
        if (capacity > values.length) {
            values = Arrays.copyOf(values, Math.min(BLOCK_SIZE, Math.max(capacity, size * 2)));
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(final int offset) {
        return (bits[offset >>> 6] & (1L << offset)) != 0;
    }

    @Override
    public short get(final int offset) {
        if ((bits[offset >>> 6] & (1L << offset)) == 0) {
            return NULL_SHORT;
        }
        return values[rank(offset)];
    }

    @Override
    public void put(final int offset, final short value) {
        final int word = offset >>> 6;
        final long bit = 1L << offset;
        final int index = rank(offset);
        if ((bits[word] & bit) != 0) {
            values[index] = value;
            return;
        }
        ensureCapacity(size + 1);
        if (index < size) {
            System.arraycopy(values, index, values, index + 1, size - index);
        }
        values[index] = value;
        bits[word] |= bit;
        adjustRanksAfter(word, 1);
        ++size;
    }

    @Override
    public void putSorted(final int[] offsets, final ShortChunk<?> src, final int srcOffset, final int count) {
        if (count == 1) {
            put(offsets[0], src.get(srcOffset));
            return;
        }
        final int newSize = size + countAbsent(offsets, count);
        ensureCapacity(newSize);
        // merge from the highest offset down, walking the existing offsets through the bitmap, so that each value
        // moves at most once and lands above any value not yet moved
        int index = size - 1;
        int dest = newSize - 1;
        int wi = WORDS - 1;
        long wordBits = bits[wi];
        for (int ii = count - 1; ii >= 0;) {
            while (wordBits == 0 && wi > 0) {
                wordBits = bits[--wi];
            }
            final int existing = wordBits == 0 ? -1 : (wi << 6) + 63 - Long.numberOfLeadingZeros(wordBits);
            final int offset = offsets[ii];
            if (existing > offset) {
                values[dest] = values[index--];
                wordBits &= ~(1L << existing);
            } else {
                if (existing == offset) {
                    --index;
                    wordBits &= ~(1L << existing);
                }
                values[dest] = src.get(srcOffset + ii--);
            }
            --dest;
        }
        for (int ii = 0; ii < count; ++ii) {
            final int offset = offsets[ii];
            bits[offset >>> 6] |= 1L << offset;
        }
        int rank = 0;
        for (int ii = 0; ii < WORDS; ++ii) {
            rankBefore[ii] = (short) rank;
            rank += Long.bitCount(bits[ii]);
        }
        size = newSize;
    }

    @Override
    public void remove(final int offset) {
        final int word = offset >>> 6;
        final long bit = 1L << offset;
        if ((bits[word] & bit) == 0) {
            return;
        }
        final int index = rank(offset);
        System.arraycopy(values, index + 1, values, index, size - index - 1);
        values[--size] = NULL_SHORT;
        bits[word] &= ~bit;
        adjustRanksAfter(word, -1);
    }

    @Override
    public void copyTo(final short[] block) {
        int index = 0;
        for (int wi = 0; wi < WORDS; ++wi) {
            long wordBits = bits[wi];
            while (wordBits != 0) {
                final int bitIndex = Long.numberOfTrailingZeros(wordBits);
                block[(wi << 6) + bitIndex] = values[index++];
                wordBits &= wordBits - 1;
            }
        }
    }

    @Override
    public void fillRange(
            final int firstOffset,
            final int lastOffset,
            final WritableShortChunk<?> dest,
            final int destOffset) {
        // the values for consecutive offsets are consecutive, so only the first rank needs counting
        int index = rank(firstOffset);
        for (int offset = firstOffset, di = destOffset; offset <= lastOffset; ++offset, ++di) {
            if ((bits[offset >>> 6] & (1L << offset)) != 0) {
                dest.set(di, values[index++]);
            } else {
                dest.set(di, NULL_SHORT);
            }
        }
    }

    @Override
    public long estimateSize() {
        // object header and fields, the bitmap, the ranks, and the values, each array with a 16 byte header
        return 32 + (16 + (long) WORDS * Long.BYTES) + (16 + (long) WORDS * Short.BYTES)
                + (16 + (long) values.length * Short.BYTES);
    }
}
