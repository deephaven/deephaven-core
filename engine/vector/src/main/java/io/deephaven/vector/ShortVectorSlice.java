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
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.base.ClampUtil.clampLong;
import static io.deephaven.vector.Vector.clampIndex;

/**
 * A subset of a {@link ShortVector} according to a range of positions.
 */
public class ShortVectorSlice extends ShortVector.Indirect {

    private final ShortVector innerVector;
    private final long offsetIndex;
    private final long length;
    private final long innerVectorValidFromInclusive;
    private final long innerVectorValidToExclusive;

    public ShortVectorSlice(
            @NotNull final ShortVector innerVector,
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

    public ShortVectorSlice(
            @NotNull final ShortVector innerVector,
            final long offsetIndex,
            final long length) {
        this(innerVector, offsetIndex, length,
                clampLong(0, innerVector.size(), offsetIndex),
                clampLong(0, innerVector.size(), offsetIndex + length));
    }

    @Override
    public short get(final long index) {
        return innerVector
                .get(clampIndex(innerVectorValidFromInclusive, innerVectorValidToExclusive, index + offsetIndex));
    }

    @Override
    public ShortVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        final long newLength = toIndexExclusive - fromIndexInclusive;
        final long newOffsetIndex = offsetIndex + fromIndexInclusive;
        return new ShortVectorSlice(innerVector, newOffsetIndex, newLength,
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex),
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex + newLength));
    }

    @Override
    public ShortVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Arrays.stream(positions)
                .map(p -> clampIndex(innerVectorValidFromInclusive, innerVectorValidToExclusive, p + offsetIndex))
                .toArray());
    }

    @Override
    public short[] toArray() {
        if (innerVector instanceof ShortVectorDirect
                && offsetIndex >= innerVectorValidFromInclusive
                && offsetIndex + length <= innerVectorValidToExclusive) {
            return Arrays.copyOfRange(innerVector.toArray(),
                    LongSizedDataStructure.intSize("ShortVectorSlice.toArray", offsetIndex),
                    LongSizedDataStructure.intSize("ShortVectorSlice.toArray", offsetIndex + length));
        }
        final short[] result = new short[LongSizedDataStructure.intSize("ShortVectorSlice.toArray", length)];
        for (int ii = 0; ii < length; ++ii) {
            result[ii] = get(ii);
        }
        return result;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public boolean isEmpty() {
        return length == 0;
    }
}
