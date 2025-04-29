//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfChar;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.base.ClampUtil.clampLong;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.vector.Vector.clampIndex;

/**
 * A subset of a {@link CharVector} according to a range of positions.
 */
public class CharVectorSlice extends CharVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final CharVector innerVector;
    private final long offsetIndex;
    private final long length;
    private final long innerVectorValidFromInclusive;
    private final long innerVectorValidToExclusive;

    private CharVectorSlice(
            @NotNull final CharVector innerVector,
            final long offsetIndex,
            final long length,
            final long innerVectorValidFromInclusive,
            final long innerVectorValidToExclusive) {
        Assert.geqZero(length, "length");
        Assert.leq(innerVectorValidFromInclusive, "innerArrayValidFromInclusive",
                innerVectorValidToExclusive, "innerArrayValidToExclusive");
        this.innerVector = innerVector;
        this.offsetIndex = offsetIndex;
        this.length = length;
        this.innerVectorValidFromInclusive = innerVectorValidFromInclusive;
        this.innerVectorValidToExclusive = innerVectorValidToExclusive;
    }

    public CharVectorSlice(
            @NotNull final CharVector innerVector,
            final long offsetIndex,
            final long length) {
        this(innerVector, offsetIndex, length,
                clampLong(0, innerVector.size(), offsetIndex),
                clampLong(0, innerVector.size(), offsetIndex + length));
    }

    @Override
    public char get(final long index) {
        return innerVector
                .get(clampIndex(innerVectorValidFromInclusive, innerVectorValidToExclusive, index + offsetIndex));
    }

    @Override
    public CharVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        final long newLength = toIndexExclusive - fromIndexInclusive;
        final long newOffsetIndex = offsetIndex + fromIndexInclusive;
        return new CharVectorSlice(innerVector, newOffsetIndex, newLength,
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex),
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex + newLength));
    }

    @Override
    public CharVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Arrays.stream(positions)
                .map((final long position) -> clampIndex(
                        innerVectorValidFromInclusive,
                        innerVectorValidToExclusive,
                        position + offsetIndex))
                .toArray());
    }

    @Override
    public char[] toArray() {
        if (innerVector instanceof CharVectorDirect
                && offsetIndex >= innerVectorValidFromInclusive
                && offsetIndex + length <= innerVectorValidToExclusive) {
            // In this case, innerVectorValidFromInclusive must be in range [0, MAX_ARRAY_SIZE) and
            // innerVectorValidToExclusive must be in range [0, MAX_ARRAY_SIZE].
            return Arrays.copyOfRange(innerVector.toArray(), (int) offsetIndex, (int) (offsetIndex + length));
        }
        return super.toArray();
    }

    @Override
    public ValueIteratorOfChar iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        final long totalWanted = toIndexExclusive - fromIndexInclusive;
        long nextIndexWanted = fromIndexInclusive + offsetIndex;

        final long includedInitialNulls = nextIndexWanted < innerVectorValidFromInclusive
                ? Math.min(innerVectorValidFromInclusive - nextIndexWanted, totalWanted)
                : 0;
        long remaining = totalWanted - includedInitialNulls;
        nextIndexWanted += includedInitialNulls;

        final long firstIncludedInnerOffset;
        final long includedInnerLength;
        if (nextIndexWanted < innerVectorValidToExclusive) {
            firstIncludedInnerOffset = nextIndexWanted;
            includedInnerLength = Math.min(innerVectorValidToExclusive - nextIndexWanted, remaining);
            remaining -= includedInnerLength;
            // Unused, but note for posterity:
            // nextIndexWanted += includedInnerLength;
        } else {
            firstIncludedInnerOffset = -1;
            includedInnerLength = 0;
        }

        final ValueIteratorOfChar innerIterator = includedInnerLength > 0
                ? innerVector.iterator(firstIncludedInnerOffset, firstIncludedInnerOffset + includedInnerLength)
                : null;
        return ValueIteratorOfChar.wrapWithNulls(innerIterator, includedInitialNulls, remaining);
    }

    @Override
    public long size() {
        return length;
    }
}
