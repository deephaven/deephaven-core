//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

/**
 * Binary searches over the sorted, de-duplicated right range values of a {@link RangeSearchKernel}, ordering values the
 * way the kernels compare them. Each search returns the index of {@code key} in
 * {@code [fromIndexInclusive, toIndexExclusive)}, or {@code ~insertionPoint} if it is absent.
 * <p>
 * The right range values never hold {@code null} or {@code NaN}. For the integral and {@code Object} types, the chunk's
 * own search agrees with the kernels' ordering. For {@code float} and {@code double}, the chunk's search uses Java's
 * total ordering, which places {@code -0.0} before {@code 0.0}; the kernels, like Deephaven ordering generally, treat
 * the two as equal, so those searches compare with {@code <} alone.
 */
final class RangeSearchBinarySearch {

    private RangeSearchBinarySearch() {}

    static int binarySearch(
            @NotNull final CharChunk<? extends Values> values,
            final int fromIndexInclusive,
            final int toIndexExclusive,
            final char key) {
        return values.binarySearch(fromIndexInclusive, toIndexExclusive, key);
    }

    static int binarySearch(
            @NotNull final ByteChunk<? extends Values> values,
            final int fromIndexInclusive,
            final int toIndexExclusive,
            final byte key) {
        return values.binarySearch(fromIndexInclusive, toIndexExclusive, key);
    }

    static int binarySearch(
            @NotNull final ShortChunk<? extends Values> values,
            final int fromIndexInclusive,
            final int toIndexExclusive,
            final short key) {
        return values.binarySearch(fromIndexInclusive, toIndexExclusive, key);
    }

    static int binarySearch(
            @NotNull final IntChunk<? extends Values> values,
            final int fromIndexInclusive,
            final int toIndexExclusive,
            final int key) {
        return values.binarySearch(fromIndexInclusive, toIndexExclusive, key);
    }

    static int binarySearch(
            @NotNull final LongChunk<? extends Values> values,
            final int fromIndexInclusive,
            final int toIndexExclusive,
            final long key) {
        return values.binarySearch(fromIndexInclusive, toIndexExclusive, key);
    }

    static int binarySearch(
            @NotNull final ObjectChunk<Object, ? extends Values> values,
            final int fromIndexInclusive,
            final int toIndexExclusive,
            final Object key) {
        return values.binarySearch(fromIndexInclusive, toIndexExclusive, key);
    }

    static int binarySearch(
            @NotNull final FloatChunk<? extends Values> values,
            final int fromIndexInclusive,
            final int toIndexExclusive,
            final float key) {
        int lowIndex = fromIndexInclusive;
        int highIndex = toIndexExclusive - 1;
        while (lowIndex <= highIndex) {
            final int midIndex = (lowIndex + highIndex) >>> 1;
            final float midValue = values.get(midIndex);
            if (midValue < key) {
                lowIndex = midIndex + 1;
            } else if (key < midValue) {
                highIndex = midIndex - 1;
            } else {
                return midIndex;
            }
        }
        return ~lowIndex;
    }

    static int binarySearch(
            @NotNull final DoubleChunk<? extends Values> values,
            final int fromIndexInclusive,
            final int toIndexExclusive,
            final double key) {
        int lowIndex = fromIndexInclusive;
        int highIndex = toIndexExclusive - 1;
        while (lowIndex <= highIndex) {
            final int midIndex = (lowIndex + highIndex) >>> 1;
            final double midValue = values.get(midIndex);
            if (midValue < key) {
                lowIndex = midIndex + 1;
            } else if (key < midValue) {
                highIndex = midIndex - 1;
            } else {
                return midIndex;
            }
        }
        return ~lowIndex;
    }
}
