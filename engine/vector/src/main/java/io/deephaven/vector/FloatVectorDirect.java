//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorDirect and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfFloat;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * A {@link FloatVector} backed by an array.
 */
@ArrayType(type = float[].class)
public final class FloatVectorDirect implements FloatVector {

    private final static long serialVersionUID = 3636374971797603565L;

    public static final FloatVector ZERO_LENGTH_VECTOR = new FloatVectorDirect();

    private final float[] data;

    public FloatVectorDirect(@NotNull final float... data) {
        this.data = Require.neqNull(data, "data");
    }

    @Override
    public float get(final long index) {
        if (index < 0 || index >= data.length) {
            return NULL_FLOAT;
        }
        return data[(int) index];
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
    public float[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    @Override
    public ValueIteratorOfFloat iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return ValueIteratorOfFloat.of(data);
        }
        return FloatVector.super.iterator(fromIndexInclusive, toIndexExclusive);
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
    public String toString() {
        return FloatVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof FloatVectorDirect) {
            return Arrays.equals(data, ((FloatVectorDirect) obj).data);
        }
        return FloatVector.equals(this, obj);
    }

    @Override
    public int hashCode() {
        return FloatVector.hashCode(this);
    }
}
