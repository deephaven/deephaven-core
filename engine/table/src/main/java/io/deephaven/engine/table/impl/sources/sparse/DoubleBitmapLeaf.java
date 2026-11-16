//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBitmapLeaf and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.INDEX_MASK;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * A {@link DoubleCompactLeaf} that records which offsets hold a value in a bitmap and stores the values densely in offset
 * order. The value for an offset is at its rank: the number of set bits before it, which is the stored rank at the
 * start of the offset's bitmap word plus the set bits before the offset within that word.
 */
public final class DoubleBitmapLeaf extends DoubleCompactLeaf {
    private static final int WORDS = BLOCK_SIZE / Long.SIZE;

    /** A leaf holding no values. */
    public static final DoubleBitmapLeaf EMPTY = new DoubleBitmapLeaf(new long[WORDS], new double[0]);

    /** Bit {@code offset % 64} of word {@code offset / 64} is set when {@code offset} holds a value. */
    private final long[] bits;
    /** The number of values in the words before each word. */
    private final short[] rankBefore;
    /** The values, in offset order. */
    private final double[] values;

    /**
     * Make a leaf of the offsets in {@code bits} and their values. The arrays are complete before the leaf is made, so
     * that a reader on another thread that finds the leaf sees them in full.
     *
     * @param bits the offsets that hold a value
     * @param values the values of those offsets, in offset order, one for each set bit of {@code bits}
     */
    DoubleBitmapLeaf(final long[] bits, final double[] values) {
        this.bits = bits;
        this.values = values;
        rankBefore = new short[WORDS];
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
    public double get(final int offset) {
        if ((bits[offset >>> 6] & (1L << offset)) == 0) {
            return NULL_DOUBLE;
        }
        return values[rank(offset)];
    }

    @Override
    public DoubleCompactLeaf put(final int offset, final double value, final int sortedThreshold) {
        final int index = rank(offset);
        if (contains(offset)) {
            values[index] = value;
            return this;
        }
        final long[] newBits = bits.clone();
        newBits[offset >>> 6] |= 1L << offset;
        final double[] newValues = new double[values.length + 1];
        System.arraycopy(values, 0, newValues, 0, index);
        newValues[index] = value;
        System.arraycopy(values, index, newValues, index + 1, values.length - index);
        return new DoubleBitmapLeaf(newBits, newValues);
    }

    @Override
    public DoubleCompactLeaf putSorted(final int[] offsets, final DoubleChunk<?> src, final int srcOffset,
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
        return merge(offsets, src, srcOffset, count, values.length + absent);
    }

    @Override
    public DoubleCompactLeaf insertAbsent(final int[] offsets, final int count, final int sortedThreshold) {
        final int absent = countAbsent(offsets, count);
        return absent == 0 ? this : merge(offsets, null, 0, count, values.length + absent);
    }

    /**
     * @return a new leaf of {@code newSize} values holding this leaf's offsets and {@code offsets[0, count)}, ascending
     *         and distinct, with the values of {@code src} from {@code srcOffset} for those offsets, or, if {@code src}
     *         is null, keeping the values of the offsets this leaf holds and null for the others
     */
    private DoubleBitmapLeaf merge(
            final int[] offsets,
            final DoubleChunk<?> src,
            final int srcOffset,
            final int count,
            final int newSize) {
        final long[] newBits = bits.clone();
        for (int ii = 0; ii < count; ++ii) {
            final int offset = offsets[ii];
            newBits[offset >>> 6] |= 1L << offset;
        }
        // walk the offsets of the result in ascending order, taking each value from src or from this leaf
        final double[] newValues = new double[newSize];
        int index = 0;
        int ii = 0;
        int dest = 0;
        for (int wi = 0; wi < WORDS; ++wi) {
            long wordBits = newBits[wi];
            while (wordBits != 0) {
                final int offset = (wi << 6) + Long.numberOfTrailingZeros(wordBits);
                final boolean present = (bits[wi] & (1L << offset)) != 0;
                if (ii < count && offsets[ii] == offset) {
                    if (src != null) {
                        newValues[dest] = src.get(srcOffset + ii);
                    } else {
                        newValues[dest] = present ? values[index] : NULL_DOUBLE;
                    }
                    ++ii;
                } else {
                    newValues[dest] = values[index];
                }
                if (present) {
                    ++index;
                }
                ++dest;
                wordBits &= wordBits - 1;
            }
        }
        return new DoubleBitmapLeaf(newBits, newValues);
    }

    @Override
    public DoubleCompactLeaf remove(final int offset) {
        if (!contains(offset)) {
            return this;
        }
        final int index = rank(offset);
        final long[] newBits = bits.clone();
        newBits[offset >>> 6] &= ~(1L << offset);
        final double[] newValues = new double[values.length - 1];
        System.arraycopy(values, 0, newValues, 0, index);
        System.arraycopy(values, index + 1, newValues, index, values.length - index - 1);
        return new DoubleBitmapLeaf(newBits, newValues);
    }

    @Override
    public DoubleCompactLeaf removeRange(final int firstOffset, final int lastOffset) {
        final long[] newBits = bits.clone();
        int removed = 0;
        for (int wi = firstOffset >>> 6; wi <= lastOffset >>> 6; ++wi) {
            final long cleared = newBits[wi] & rangeMask(wi, firstOffset, lastOffset);
            removed += Long.bitCount(cleared);
            newBits[wi] &= ~cleared;
        }
        if (removed == 0) {
            return this;
        }
        // the removed values are consecutive, starting at the rank of the first offset
        final int start = rank(firstOffset);
        final double[] newValues = new double[values.length - removed];
        System.arraycopy(values, 0, newValues, 0, start);
        System.arraycopy(values, start + removed, newValues, start, values.length - start - removed);
        return new DoubleBitmapLeaf(newBits, newValues);
    }

    @Override
    public void copyTo(final double[] block) {
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
            final WritableDoubleChunk<?> dest,
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
            final WritableDoubleChunk<?> dest,
            final int destOffset) {
        // the values for consecutive offsets are consecutive, so only the first rank needs counting
        int index = rank(firstOffset);
        for (int offset = firstOffset, di = destOffset; offset <= lastOffset; ++offset, ++di) {
            if ((bits[offset >>> 6] & (1L << offset)) != 0) {
                dest.set(di, values[index++]);
            } else {
                dest.set(di, NULL_DOUBLE);
            }
        }
    }

    @Override
    public void fillKeys(
            final LongChunk<? extends RowKeys> keys,
            final int start,
            final int end,
            final WritableDoubleChunk<?> dest,
            final int destOffset) {
        for (int ii = start, di = destOffset; ii < end; ++ii, ++di) {
            final int offset = (int) (keys.get(ii) & INDEX_MASK);
            final int word = offset >>> 6;
            final long wordBits = bits[word];
            final long bit = 1L << offset;
            dest.set(di, (wordBits & bit) == 0 ? NULL_DOUBLE
                    : values[rankBefore[word] + Long.bitCount(wordBits & (bit - 1))]);
        }
    }

    @Override
    public void overlayKeys(
            final LongChunk<? extends RowKeys> keys,
            final int start,
            final int end,
            final WritableDoubleChunk<?> dest,
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
                + (16 + (long) values.length * Double.BYTES);
    }
}
