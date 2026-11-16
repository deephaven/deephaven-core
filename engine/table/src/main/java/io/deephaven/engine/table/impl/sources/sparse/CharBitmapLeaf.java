//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.INDEX_MASK;
import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * A {@link CharCompactLeaf} that records which offsets hold a value in a bitmap and stores the values densely in offset
 * order. The value for an offset is at its rank: the number of set bits before it, which is the stored rank at the
 * start of the offset's bitmap word plus the set bits before the offset within that word.
 */
public final class CharBitmapLeaf extends CharCompactLeaf {
    private static final int WORDS = BLOCK_SIZE / Long.SIZE;

    /** A leaf holding no values. */
    public static final CharBitmapLeaf EMPTY = new CharBitmapLeaf();

    /** Bit {@code offset % 64} of word {@code offset / 64} is set when {@code offset} holds a value. */
    private final long[] bits;
    /** The number of values in the words before each word. */
    private final short[] rankBefore;
    /** The values, in offset order. */
    private final char[] values;

    /*
     * Each constructor allocates the leaf's arrays after the leaf, in the order a read visits them (the bitmap, the
     * ranks, the values), so that a leaf and its arrays lie together in memory, and fills them before it returns, so
     * that a reader on another thread that finds the leaf sees them in full.
     */

    /**
     * Make a leaf holding no values.
     */
    private CharBitmapLeaf() {
        bits = new long[WORDS];
        rankBefore = new short[WORDS];
        values = new char[0];
    }

    /**
     * Make a leaf of {@code newSize} values holding the offsets of {@code base} and {@code offsets[0, count)},
     * ascending and distinct, with the values of {@code src} from {@code srcOffset} for those offsets, or, if
     * {@code src} is null, keeping the values of the offsets {@code base} holds and null for the others.
     */
    CharBitmapLeaf(
            final CharCompactLeaf base,
            final int[] offsets,
            final CharChunk<?> src,
            final int srcOffset,
            final int count,
            final int newSize) {
        bits = new long[WORDS];
        rankBefore = new short[WORDS];
        values = new char[newSize];
        if (base instanceof CharBitmapLeaf) {
            mergeBitmap((CharBitmapLeaf) base, offsets, src, srcOffset, count);
        } else {
            mergeSorted((CharSortedLeaf) base, offsets, src, srcOffset, count);
        }
        computeRanks();
    }

    /**
     * Make a leaf holding the offsets and values of {@code base} and {@code value} at {@code offset}, which
     * {@code base} does not hold, and whose value belongs at {@code index} among the values in offset order.
     */
    CharBitmapLeaf(final CharCompactLeaf base, final int offset, final char value, final int index) {
        bits = new long[WORDS];
        rankBefore = new short[WORDS];
        final int baseSize = base.size();
        values = new char[baseSize + 1];
        if (base instanceof CharBitmapLeaf) {
            final CharBitmapLeaf bitmap = (CharBitmapLeaf) base;
            System.arraycopy(bitmap.bits, 0, bits, 0, WORDS);
            System.arraycopy(bitmap.values, 0, values, 0, index);
            System.arraycopy(bitmap.values, index, values, index + 1, baseSize - index);
        } else {
            final CharSortedLeaf sorted = (CharSortedLeaf) base;
            for (int ii = 0; ii < baseSize; ++ii) {
                setBit(sorted.offsetAt(ii));
                values[ii < index ? ii : ii + 1] = sorted.valueAt(ii);
            }
        }
        setBit(offset);
        values[index] = value;
        computeRanks();
    }

    /**
     * Make a leaf holding the offsets and values of {@code base} except those from {@code firstOffset} through
     * {@code lastOffset} inclusive.
     */
    private CharBitmapLeaf(final CharBitmapLeaf base, final int firstOffset, final int lastOffset) {
        bits = base.bits.clone();
        int removed = 0;
        for (int wi = firstOffset >>> 6; wi <= lastOffset >>> 6; ++wi) {
            final long cleared = bits[wi] & rangeMask(wi, firstOffset, lastOffset);
            removed += Long.bitCount(cleared);
            bits[wi] &= ~cleared;
        }
        rankBefore = new short[WORDS];
        values = new char[base.values.length - removed];
        // the removed values are consecutive, starting at the rank of the first offset
        final int start = base.rank(firstOffset);
        System.arraycopy(base.values, 0, values, 0, start);
        System.arraycopy(base.values, start + removed, values, start, values.length - start);
        computeRanks();
    }

    /**
     * Fill this leaf's bitmap and values with those of {@code base} merged with {@code offsets[0, count)}, as the
     * merging constructor describes.
     */
    private void mergeBitmap(
            final CharBitmapLeaf base,
            final int[] offsets,
            final CharChunk<?> src,
            final int srcOffset,
            final int count) {
        System.arraycopy(base.bits, 0, bits, 0, WORDS);
        for (int ii = 0; ii < count; ++ii) {
            setBit(offsets[ii]);
        }
        // walk the offsets of the result in ascending order, taking each value from src or from base
        int index = 0;
        int ii = 0;
        int dest = 0;
        for (int wi = 0; wi < WORDS; ++wi) {
            long wordBits = bits[wi];
            while (wordBits != 0) {
                final int offset = (wi << 6) + Long.numberOfTrailingZeros(wordBits);
                final boolean present = (base.bits[wi] & (1L << offset)) != 0;
                if (ii < count && offsets[ii] == offset) {
                    if (src != null) {
                        values[dest] = src.get(srcOffset + ii);
                    } else {
                        values[dest] = present ? base.values[index] : NULL_CHAR;
                    }
                    ++ii;
                } else {
                    values[dest] = base.values[index];
                }
                if (present) {
                    ++index;
                }
                ++dest;
                wordBits &= wordBits - 1;
            }
        }
    }

    /**
     * Fill this leaf's bitmap and values with those of {@code base}, a sorted leaf, merged with
     * {@code offsets[0, count)}, as the merging constructor describes.
     */
    private void mergeSorted(
            final CharSortedLeaf base,
            final int[] offsets,
            final CharChunk<?> src,
            final int srcOffset,
            final int count) {
        final int baseSize = base.size();
        int index = 0;
        int dest = 0;
        for (int ii = 0; ii < count; ++ii) {
            final int offset = offsets[ii];
            int existing;
            while (index < baseSize && (existing = base.offsetAt(index)) < offset) {
                setBit(existing);
                values[dest++] = base.valueAt(index++);
            }
            final boolean present = index < baseSize && base.offsetAt(index) == offset;
            setBit(offset);
            if (src != null) {
                values[dest++] = src.get(srcOffset + ii);
            } else {
                values[dest++] = present ? base.valueAt(index) : NULL_CHAR;
            }
            if (present) {
                ++index;
            }
        }
        while (index < baseSize) {
            setBit(base.offsetAt(index));
            values[dest++] = base.valueAt(index++);
        }
    }

    private void setBit(final int offset) {
        bits[offset >>> 6] |= 1L << offset;
    }

    /**
     * Set the rank of each bitmap word from the bitmap.
     */
    private void computeRanks() {
        int rank = 0;
        for (int wi = 0; wi < WORDS; ++wi) {
            rankBefore[wi] = (short) rank;
            rank += Long.bitCount(bits[wi]);
        }
    }

    /**
     * @return the number of values at offsets before {@code offset}
     */
    private int rank(final int offset) {
        final int word = offset >>> 6;
        return rankBefore[word] + Long.bitCount(bits[word] & ((1L << offset) - 1));
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public boolean contains(final int offset) {
        return (bits[offset >>> 6] & (1L << offset)) != 0;
    }

    @Override
    public int countAbsent(final int[] offsets, final int count) {
        int absent = 0;
        for (int ii = 0; ii < count; ++ii) {
            if (!contains(offsets[ii])) {
                ++absent;
            }
        }
        return absent;
    }

    @Override
    public char get(final int offset) {
        if ((bits[offset >>> 6] & (1L << offset)) == 0) {
            return NULL_CHAR;
        }
        return values[rank(offset)];
    }

    @Override
    public CharCompactLeaf put(final int offset, final char value, final int sortedThreshold) {
        final int index = rank(offset);
        if (contains(offset)) {
            values[index] = value;
            return this;
        }
        return new CharBitmapLeaf(this, offset, value, index);
    }

    @Override
    public CharCompactLeaf putSorted(final int[] offsets, final CharChunk<?> src, final int srcOffset,
            final int count, final int sortedThreshold) {
        if (count == 1) {
            return put(offsets[0], src.get(srcOffset), sortedThreshold);
        }
        final int absent = countAbsent(offsets, count);
        if (absent == 0) {
            for (int ii = 0; ii < count; ++ii) {
                values[rank(offsets[ii])] = src.get(srcOffset + ii);
            }
            return this;
        }
        return new CharBitmapLeaf(this, offsets, src, srcOffset, count, values.length + absent);
    }

    @Override
    public CharCompactLeaf insertAbsent(final int[] offsets, final int count, final int sortedThreshold) {
        final int absent = countAbsent(offsets, count);
        return absent == 0 ? this : new CharBitmapLeaf(this, offsets, null, 0, count, values.length + absent);
    }

    @Override
    public CharCompactLeaf remove(final int offset) {
        return contains(offset) ? new CharBitmapLeaf(this, offset, offset) : this;
    }

    @Override
    public CharCompactLeaf removeRange(final int firstOffset, final int lastOffset) {
        for (int wi = firstOffset >>> 6; wi <= lastOffset >>> 6; ++wi) {
            if ((bits[wi] & rangeMask(wi, firstOffset, lastOffset)) != 0) {
                return new CharBitmapLeaf(this, firstOffset, lastOffset);
            }
        }
        return this;
    }

    @Override
    public void copyTo(final char[] block) {
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
    public int copyOffsets(final int firstOffset, final int lastOffset, final int[] dest) {
        int count = 0;
        for (int wi = firstOffset >>> 6; wi <= lastOffset >>> 6; ++wi) {
            long wordBits = bits[wi] & rangeMask(wi, firstOffset, lastOffset);
            while (wordBits != 0) {
                dest[count++] = (wi << 6) + Long.numberOfTrailingZeros(wordBits);
                wordBits &= wordBits - 1;
            }
        }
        return count;
    }

    @Override
    public void overlayRange(
            final int firstOffset,
            final int lastOffset,
            final WritableCharChunk<?> dest,
            final int destOffset) {
        int index = rank(firstOffset);
        for (int wi = firstOffset >>> 6; wi <= lastOffset >>> 6; ++wi) {
            long wordBits = bits[wi] & rangeMask(wi, firstOffset, lastOffset);
            while (wordBits != 0) {
                final int offset = (wi << 6) + Long.numberOfTrailingZeros(wordBits);
                dest.set(destOffset + offset - firstOffset, values[index++]);
                wordBits &= wordBits - 1;
            }
        }
    }

    /**
     * @return the bits of word {@code wi} for offsets from {@code firstOffset} through {@code lastOffset} inclusive
     */
    private static long rangeMask(final int wi, final int firstOffset, final int lastOffset) {
        final long fromFirst = wi == firstOffset >>> 6 ? -1L << firstOffset : -1L;
        final long throughLast = wi == lastOffset >>> 6 ? -1L >>> (63 - (lastOffset & 63)) : -1L;
        return fromFirst & throughLast;
    }

    @Override
    public void fillRange(
            final int firstOffset,
            final int lastOffset,
            final WritableCharChunk<?> dest,
            final int destOffset) {
        // the values for consecutive offsets are consecutive, so only the first rank needs counting
        int index = rank(firstOffset);
        for (int offset = firstOffset, di = destOffset; offset <= lastOffset; ++offset, ++di) {
            if ((bits[offset >>> 6] & (1L << offset)) != 0) {
                dest.set(di, values[index++]);
            } else {
                dest.set(di, NULL_CHAR);
            }
        }
    }

    @Override
    public void fillKeys(
            final LongChunk<? extends RowKeys> keys,
            final int start,
            final int end,
            final WritableCharChunk<?> dest,
            final int destOffset) {
        for (int ii = start, di = destOffset; ii < end; ++ii, ++di) {
            final int offset = (int) (keys.get(ii) & INDEX_MASK);
            final int word = offset >>> 6;
            final long wordBits = bits[word];
            final long bit = 1L << offset;
            dest.set(di, (wordBits & bit) == 0 ? NULL_CHAR
                    : values[rankBefore[word] + Long.bitCount(wordBits & (bit - 1))]);
        }
    }

    @Override
    public void overlayKeys(
            final LongChunk<? extends RowKeys> keys,
            final int start,
            final int end,
            final WritableCharChunk<?> dest,
            final int destOffset) {
        for (int ii = start, di = destOffset; ii < end; ++ii, ++di) {
            final int offset = (int) (keys.get(ii) & INDEX_MASK);
            final int word = offset >>> 6;
            final long wordBits = bits[word];
            final long bit = 1L << offset;
            if ((wordBits & bit) != 0) {
                dest.set(di, values[rankBefore[word] + Long.bitCount(wordBits & (bit - 1))]);
            }
        }
    }

    @Override
    public long estimateSize() {
        // object header and fields, the bitmap, the ranks, and the values, each array with a 16 byte header
        return 32 + (16 + (long) WORDS * Long.BYTES) + (16 + (long) WORDS * Short.BYTES)
                + (16 + (long) values.length * Character.BYTES);
    }
}
