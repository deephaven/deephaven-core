//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorSlice and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfByte;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.base.ClampUtil.clampLong;
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.vector.Vector.clampIndex;

/**
 * A subset of a {@link ByteVector} according to a range of positions.
 */
public class ByteVectorSlice extends ByteVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final ByteVector innerVector;
    private final long offsetIndex;
    private final long length;
    private final long innerVectorValidFromInclusive;
    private final long innerVectorValidToExclusive;

    private ByteVectorSlice(
            @NotNull final ByteVector innerVector,
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

    public ByteVectorSlice(
            @NotNull final ByteVector innerVector,
            final long offsetIndex,
            final long length) {
        this(innerVector, offsetIndex, length,
                clampLong(0, innerVector.size(), offsetIndex),
                clampLong(0, innerVector.size(), offsetIndex + length));
    }

    @Override
    public byte get(final long index) {
        return innerVector
                .get(clampIndex(innerVectorValidFromInclusive, innerVectorValidToExclusive, index + offsetIndex));
    }

    @Override
    public ByteVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        final long newLength = toIndexExclusive - fromIndexInclusive;
        final long newOffsetIndex = offsetIndex + fromIndexInclusive;
        return new ByteVectorSlice(innerVector, newOffsetIndex, newLength,
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex),
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex + newLength));
    }

    @Override
    public ByteVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Arrays.stream(positions)
                .map((final long position) -> clampIndex(
                        innerVectorValidFromInclusive,
                        innerVectorValidToExclusive,
                        position + offsetIndex))
                .toArray());
    }

    @Override
    public byte[] toArray() {
        if (innerVector instanceof ByteVectorDirect
                && offsetIndex >= innerVectorValidFromInclusive
                && offsetIndex + length <= innerVectorValidToExclusive) {
            // In this case, innerVectorValidFromInclusive must be in range [0, MAX_ARRAY_SIZE) and
            // innerVectorValidToExclusive must be in range [0, MAX_ARRAY_SIZE].
            return Arrays.copyOfRange(innerVector.toArray(), (int) offsetIndex, (int) (offsetIndex + length));
        }
        return super.toArray();
    }

    @Override
    public ValueIteratorOfByte iterator(final long fromIndexInclusive, final long toIndexExclusive) {
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

        final ValueIteratorOfByte innerIterator = includedInnerLength > 0
                ? innerVector.iterator(firstIncludedInnerOffset, firstIncludedInnerOffset + includedInnerLength)
                : null;
        return ValueIteratorOfByte.wrapWithNulls(innerIterator, includedInitialNulls, remaining);
    }

    @Override
    public long size() {
        return length;
    }
}
