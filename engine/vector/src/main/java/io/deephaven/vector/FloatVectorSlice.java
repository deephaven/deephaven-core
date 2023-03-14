/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorSlice and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfFloat;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.base.ClampUtil.clampLong;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.vector.Vector.clampIndex;

/**
 * A subset of a {@link FloatVector} according to a range of positions.
 */
public class FloatVectorSlice extends FloatVector.Indirect {

    private final FloatVector innerVector;
    private final long offsetIndex;
    private final long length;
    private final long innerVectorValidFromInclusive;
    private final long innerVectorValidToExclusive;

    public FloatVectorSlice(
            @NotNull final FloatVector innerVector,
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

    public FloatVectorSlice(
            @NotNull final FloatVector innerVector,
            final long offsetIndex,
            final long length) {
        this(innerVector, offsetIndex, length,
                clampLong(0, innerVector.size(), offsetIndex),
                clampLong(0, innerVector.size(), offsetIndex + length));
    }

    @Override
    public float get(final long index) {
        return innerVector
                .get(clampIndex(innerVectorValidFromInclusive, innerVectorValidToExclusive, index + offsetIndex));
    }

    @Override
    public FloatVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        final long newLength = toIndexExclusive - fromIndexInclusive;
        final long newOffsetIndex = offsetIndex + fromIndexInclusive;
        return new FloatVectorSlice(innerVector, newOffsetIndex, newLength,
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex),
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex + newLength));
    }

    @Override
    public FloatVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Arrays.stream(positions)
                .map((final long position) -> clampIndex(
                        innerVectorValidFromInclusive,
                        innerVectorValidToExclusive,
                        position + offsetIndex))
                .toArray());
    }

    @Override
    public float[] toArray() {
        if (innerVector instanceof FloatVectorDirect
                && offsetIndex >= innerVectorValidFromInclusive
                && offsetIndex + length <= innerVectorValidToExclusive) {
            // In this case, innerVectorValidFromInclusive must be in range [0, MAX_ARRAY_SIZE) and
            // innerVectorValidToExclusive must be in range [0, MAX_ARRAY_SIZE].
            return Arrays.copyOfRange(innerVector.toArray(), (int) offsetIndex, (int) (offsetIndex + length));
        }
        return super.toArray();
    }

    @Override
    public CloseablePrimitiveIteratorOfFloat iterator(long fromIndexInclusive, long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        fromIndexInclusive += offsetIndex;
        toIndexExclusive += offsetIndex;
        final long totalWanted = toIndexExclusive - fromIndexInclusive;
        final long includedInitialNulls = fromIndexInclusive < innerVectorValidFromInclusive
                ? Math.min(innerVectorValidFromInclusive - fromIndexInclusive, totalWanted)
                : 0;
        long remaining = totalWanted - includedInitialNulls;

        final long innerVectorValidSize = innerVectorValidToExclusive - innerVectorValidFromInclusive;
        final long firstIncludedInnerOffset;
        final long includedInnerLength;
        if (remaining > 0
                && innerVectorValidSize > 0
                && fromIndexInclusive < innerVectorValidFromInclusive + innerVectorValidSize) {
            if (fromIndexInclusive <= innerVectorValidFromInclusive) {
                firstIncludedInnerOffset = innerVectorValidFromInclusive;
                includedInnerLength = Math.min(innerVectorValidSize, remaining);
            } else {
                firstIncludedInnerOffset = fromIndexInclusive - innerVectorValidFromInclusive;
                includedInnerLength = Math.min(innerVectorValidSize - firstIncludedInnerOffset, remaining);
            }
            remaining -= includedInnerLength;
        } else {
            firstIncludedInnerOffset = -1;
            includedInnerLength = 0;
        }

        final CloseablePrimitiveIteratorOfFloat initialNullsIterator = includedInitialNulls > 0
                ? CloseablePrimitiveIteratorOfFloat.repeat(NULL_FLOAT, includedInitialNulls)
                : null;
        final CloseablePrimitiveIteratorOfFloat innerIterator = includedInnerLength > 0
                ? innerVector.iterator(firstIncludedInnerOffset, firstIncludedInnerOffset + includedInnerLength)
                : null;
        final CloseablePrimitiveIteratorOfFloat finalNullsIterator = remaining > 0
                ? CloseablePrimitiveIteratorOfFloat.repeat(NULL_FLOAT, remaining)
                : null;
        return CloseablePrimitiveIteratorOfFloat.maybeConcat(initialNullsIterator, innerIterator, finalNullsIterator);
    }

    @Override
    public long size() {
        return length;
    }
}
