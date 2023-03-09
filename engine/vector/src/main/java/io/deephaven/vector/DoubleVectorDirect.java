/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorDirect and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * A {@link DoubleVector} backed by an array.
 */
@ArrayType(type = double[].class)
public class DoubleVectorDirect implements DoubleVector {

    public static final DoubleVector ZERO_LENGTH_VECTOR = new DoubleVectorDirect();

    private final double[] data;

    public DoubleVectorDirect(final double... data) {
        this.data = data;
    }

    @Override
    public double get(final long index) {
        if (index < 0 || index > data.length - 1) {
            return NULL_DOUBLE;
        }
        return data[LongSizedDataStructure.intSize("DoubleVectorDirect get", index)];
    }

    @Override
    public DoubleVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new DoubleVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public DoubleVector subVectorByPositions(final long[] positions) {
        return new DoubleSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public double[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public DoubleVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DoubleVector.toString(this, 10);
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj instanceof DoubleVectorDirect) {
            return Arrays.equals(data, ((DoubleVectorDirect) obj).data);
        }
        return DoubleVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DoubleVector.hashCode(this);
    }
}
