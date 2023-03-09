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

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * A {@link FloatVector} backed by an array.
 */
@ArrayType(type = float[].class)
public class FloatVectorDirect implements FloatVector {

    public static final FloatVector ZERO_LENGTH_VECTOR = new FloatVectorDirect();

    private final float[] data;

    public FloatVectorDirect(final float... data) {
        this.data = data;
    }

    @Override
    public float get(final long index) {
        if (index < 0 || index > data.length - 1) {
            return NULL_FLOAT;
        }
        return data[LongSizedDataStructure.intSize("FloatVectorDirect get", index)];
    }

    @Override
    public FloatVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new FloatVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public FloatVector subVectorByPositions(final long[] positions) {
        return new FloatSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public float[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public FloatVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return FloatVector.toString(this, 10);
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj instanceof FloatVectorDirect) {
            return Arrays.equals(data, ((FloatVectorDirect) obj).data);
        }
        return FloatVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return FloatVector.hashCode(this);
    }
}
