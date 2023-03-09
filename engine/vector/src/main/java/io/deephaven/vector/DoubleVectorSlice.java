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
 * A subset of a {@link DoubleVector} according to a range of positions.
 */
public class DoubleVectorSlice extends DoubleVector.Indirect {

    private final DoubleVector innerVector;
    private final long offsetIndex;
    private final long length;
    private final long innerVectorValidFromInclusive;
    private final long innerVectorValidToExclusive;

    public DoubleVectorSlice(
            @NotNull final DoubleVector innerVector,
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

    public DoubleVectorSlice(
            @NotNull final DoubleVector innerVector,
            final long offsetIndex,
            final long length) {
        this(innerVector, offsetIndex, length,
                clampLong(0, innerVector.size(), offsetIndex),
                clampLong(0, innerVector.size(), offsetIndex + length));
    }

    @Override
    public double get(final long index) {
        return innerVector
                .get(clampIndex(innerVectorValidFromInclusive, innerVectorValidToExclusive, index + offsetIndex));
    }

    @Override
    public DoubleVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        final long newLength = toIndexExclusive - fromIndexInclusive;
        final long newOffsetIndex = offsetIndex + fromIndexInclusive;
        return new DoubleVectorSlice(innerVector, newOffsetIndex, newLength,
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex),
                clampLong(innerVectorValidFromInclusive, innerVectorValidToExclusive, newOffsetIndex + newLength));
    }

    @Override
    public DoubleVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Arrays.stream(positions)
                .map(p -> clampIndex(innerVectorValidFromInclusive, innerVectorValidToExclusive, p + offsetIndex))
                .toArray());
    }

    @Override
    public double[] toArray() {
        if (innerVector instanceof DoubleVectorDirect
                && offsetIndex >= innerVectorValidFromInclusive
                && offsetIndex + length <= innerVectorValidToExclusive) {
            return Arrays.copyOfRange(innerVector.toArray(),
                    LongSizedDataStructure.intSize("DoubleVectorSlice.toArray", offsetIndex),
                    LongSizedDataStructure.intSize("DoubleVectorSlice.toArray", offsetIndex + length));
        }
        final double[] result = new double[LongSizedDataStructure.intSize("DoubleVectorSlice.toArray", length)];
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
