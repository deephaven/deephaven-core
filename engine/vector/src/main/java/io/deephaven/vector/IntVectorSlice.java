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
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.base.ClampUtil.clampLong;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.vector.Vector.clampIndex;

/**
 * A subset of a {@link IntVector} according to a range of positions.
 */
public class IntVectorSlice extends IntVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final IntVector innerVector;
    private final long offsetIndex;
    private final long length;
    private final long innerVectorValidFromInclusive;
    private final long innerVectorValidToExclusive;

    private IntVectorSlice(
            @NotNull final IntVector innerVector,
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

    public IntVectorSlice(
            @NotNull final IntVector innerVector,
            final long offsetIndex,
            final long length) {
        this(innerVector, offsetIndex, length,
                clampLong(0, innerVector.size(), offsetIndex),
                clampLong(0, innerVector.size(), offsetIndex + length));
    }

    @Override
    public int get(final long index) {
        return innerVector
                .get(clampIndex(innerVectorValidFromInclusive, innerVectorValidToExclusive, index + offsetIndex));
    }

    @Override
    public IntVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        final long newLength = toIndexExclusive - fromIndexInclusive;
        final long newOffsetIndex = offsetIndex + fromIndexInclusive;
        return new IntVectorSlice(innerVector, newOffsetIndex, newLength,
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex),
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex + newLength));
    }

    @Override
    public IntVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Arrays.stream(positions)
                .map((final long position) -> clampIndex(
                        innerVectorValidFromInclusive,
                        innerVectorValidToExclusive,
                        position + offsetIndex))
                .toArray());
    }

    @Override
    public int[] toArray() {
        if (innerVector instanceof IntVectorDirect
                && offsetIndex >= innerVectorValidFromInclusive
                && offsetIndex + length <= innerVectorValidToExclusive) {
            // In this case, innerVectorValidFromInclusive must be in range [0, MAX_ARRAY_SIZE) and
            // innerVectorValidToExclusive must be in range [0, MAX_ARRAY_SIZE].
            return Arrays.copyOfRange(innerVector.toArray(), (int) offsetIndex, (int) (offsetIndex + length));
        }
        return super.toArray();
    }

    @Override
    public CloseablePrimitiveIteratorOfInt iterator(final long fromIndexInclusive, final long toIndexExclusive) {
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

        final CloseablePrimitiveIteratorOfInt initialNullsIterator = includedInitialNulls > 0
                ? CloseablePrimitiveIteratorOfInt.repeat(NULL_INT, includedInitialNulls)
                : null;
        final CloseablePrimitiveIteratorOfInt innerIterator = includedInnerLength > 0
                ? innerVector.iterator(firstIncludedInnerOffset, firstIncludedInnerOffset + includedInnerLength)
                : null;
        final CloseablePrimitiveIteratorOfInt finalNullsIterator = remaining > 0
                ? CloseablePrimitiveIteratorOfInt.repeat(NULL_INT, remaining)
                : null;
        return CloseablePrimitiveIteratorOfInt.maybeConcat(initialNullsIterator, innerIterator, finalNullsIterator);
    }

    @Override
    public long size() {
        return length;
    }
}
