//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBitmapLeaf and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.INDEX_MASK;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * A {@link ShortCompactLeaf} that records which offsets hold a value in a bitmap and stores the values densely in offset
 * order. The value for an offset is at its rank: the number of set bits before it, which is the stored rank at the
 * start of the offset's bitmap word plus the set bits before the offset within that word.
 */
public final class ShortBitmapLeaf extends ShortCompactLeaf {
    static final int WORDS = BLOCK_SIZE / Long.SIZE;

    /** Bit {@code offset % 64} of word {@code offset / 64} is set when {@code offset} holds a value. */
    private final long[] bits;
    /** The number of values in the words before each word. */
    private final short[] rankBefore;
    /** The values, in offset order. */
    private final short[] values;

    /*
     * Each constructor allocates the leaf's arrays after the leaf, in the order a read visits them (the bitmap, the
     * ranks, the values), so that a leaf and its arrays lie together in memory, and fills them before it returns, so
     * that a reader on another thread that finds the leaf sees them in full.
     */

    /**
     * Make a leaf of {@code newSize} values holding the offsets of {@code base} and {@code offsets[0, count)},
     * ascending and distinct, with the values of {@code src} from {@code srcOffset} for those offsets, or, if
     * {@code src} is null, keeping the values of the offsets {@code base} holds and null for the others.
     */
    ShortBitmapLeaf(
            final ShortCompactLeaf base,
            final int[] offsets,
            final ShortChunk<?> src,
            final int srcOffset,
            final int count,
            final int newSize) {
        bits = new long[WORDS];
        rankBefore = new short[WORDS];
        values = new short[newSize];
        if (base instanceof ShortBitmapLeaf) {
            mergeBitmap((ShortBitmapLeaf) base, offsets, src, srcOffset, count);
        } else {
            mergeSorted((ShortSortedLeaf) base, offsets, src, srcOffset, count);
        }
        computeRanks();
    }

    /**
     * Make a leaf holding the offsets and values of {@code base} and {@code value} at {@code offset}, which
     * {@code base} does not hold, and whose value belongs at {@code index} among the values in offset order.
     */
    ShortBitmapLeaf(final ShortCompactLeaf base, final int offset, final short value, final int index) {
        bits = new long[WORDS];
        rankBefore = new short[WORDS];
        final int baseSize = base.size();
        values = new short[baseSize + 1];
        if (base instanceof ShortBitmapLeaf) {
            final ShortBitmapLeaf bitmap = (ShortBitmapLeaf) base;
            System.arraycopy(bitmap.bits, 0, bits, 0, WORDS);
            System.arraycopy(bitmap.values, 0, values, 0, index);
            System.arraycopy(bitmap.values, index, values, index + 1, baseSize - index);
        } else {
            final ShortSortedLeaf sorted = (ShortSortedLeaf) base;
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
    private ShortBitmapLeaf(final ShortBitmapLeaf base, final int firstOffset, final int lastOffset) {
        bits = base.bits.clone();
        int removed = 0;
        for (int wi = firstOffset >>> 6; wi <= lastOffset >>> 6; ++wi) {
            final long cleared = bits[wi] & rangeMask(wi, firstOffset, lastOffset);
            removed += Long.bitCount(cleared);
            bits[wi] &= ~cleared;
        }
        rankBefore = new short[WORDS];
        values = new short[base.values.length - removed];
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
            final ShortBitmapLeaf base,
            final int[] offsets,
            final ShortChunk<?> src,
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
                        values[dest] = present ? base.values[index] : NULL_SHORT;
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
            final ShortSortedLeaf base,
            final int[] offsets,
            final ShortChunk<?> src,
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
                values[dest++] = present ? base.valueAt(index) : NULL_SHORT;
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
    public short get(final int offset) {
        if ((bits[offset >>> 6] & (1L << offset)) == 0) {
            return NULL_SHORT;
        }
        return values[rank(offset)];
    }

    @Override
    public ShortCompactLeaf put(final int offset, final short value, final int sortedThreshold) {
        final int index = rank(offset);
        if (contains(offset)) {
            values[index] = value;
            return this;
        }
        return new ShortBitmapLeaf(this, offset, value, index);
    }

    @Override
    public ShortCompactLeaf putSorted(final int[] offsets, final ShortChunk<?> src, final int srcOffset,
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
        return new ShortBitmapLeaf(this, offsets, src, srcOffset, count, values.length + absent);
    }

    @Override
    public ShortCompactLeaf insertAbsent(final int[] offsets, final int count, final int sortedThreshold) {
        final int absent = countAbsent(offsets, count);
        return absent == 0 ? this : new ShortBitmapLeaf(this, offsets, null, 0, count, values.length + absent);
    }

    @Override
    public ShortCompactLeaf remove(final int offset, final int sortedThreshold) {
        if (!contains(offset)) {
            return this;
        }
        return without(offset, offset, values.length - 1, sortedThreshold);
    }

    @Override
    public ShortCompactLeaf removeRange(final int firstOffset, final int lastOffset, final int sortedThreshold) {
        int removed = 0;
        for (int wi = firstOffset >>> 6; wi <= lastOffset >>> 6; ++wi) {
            removed += Long.bitCount(bits[wi] & rangeMask(wi, firstOffset, lastOffset));
        }
        if (removed == 0) {
            return this;
        }
        return without(firstOffset, lastOffset, values.length - removed, sortedThreshold);
    }

    /**
     * @return a leaf holding the {@code newSize} offsets and values of this leaf other than those from
     *         {@code firstOffset} through {@code lastOffset} inclusive: null if there are none, a sorted leaf if there
     *         are at most {@code sortedThreshold}, and otherwise a bitmap leaf
     */
    private ShortCompactLeaf without(final int firstOffset, final int lastOffset, final int newSize,
            final int sortedThreshold) {
        if (newSize == 0) {
            return null;
        }
        if (newSize <= sortedThreshold) {
            return new ShortSortedLeaf(this, firstOffset, lastOffset, newSize);
        }
        return new ShortBitmapLeaf(this, firstOffset, lastOffset);
    }

    /**
     * @return the bitmap word {@code wordIndex}, whose bit {@code offset % 64} is set when {@code offset} holds a value
     */
    long word(final int wordIndex) {
        return bits[wordIndex];
    }

    /**
     * @return the value at {@code rank} in offset order
     */
    short valueAtRank(final int rank) {
        return values[rank];
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
            final WritableShortChunk<?> dest,
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
    public void fillKeys(
            final LongChunk<? extends RowKeys> keys,
            final int start,
            final int end,
            final WritableShortChunk<?> dest,
            final int destOffset) {
        for (int ii = start, di = destOffset; ii < end; ++ii, ++di) {
            final int offset = (int) (keys.get(ii) & INDEX_MASK);
            final int word = offset >>> 6;
            final long wordBits = bits[word];
            final long bit = 1L << offset;
            dest.set(di, (wordBits & bit) == 0 ? NULL_SHORT
                    : values[rankBefore[word] + Long.bitCount(wordBits & (bit - 1))]);
        }
    }

    @Override
    public void overlayKeys(
            final LongChunk<? extends RowKeys> keys,
            final int start,
            final int end,
            final WritableShortChunk<?> dest,
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
                + (16 + (long) values.length * Short.BYTES);
    }
}
