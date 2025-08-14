//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorDirect and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfDouble;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * A {@link DoubleVector} backed by an array.
 */
@ArrayType(type = double[].class)
public final class DoubleVectorDirect implements DoubleVector {

    private final static long serialVersionUID = 3636374971797603565L;

    public static final DoubleVector ZERO_LENGTH_VECTOR = new DoubleVectorDirect();

    private final double[] data;

    public DoubleVectorDirect(@NotNull final double... data) {
        this.data = Require.neqNull(data, "data");
    }

    @Override
    public double get(final long index) {
        if (index < 0 || index >= data.length) {
            return NULL_DOUBLE;
        }
        return data[(int) index];
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
    public double[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    @Override
    public ValueIteratorOfDouble iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return ValueIteratorOfDouble.of(data);
        }
        return DoubleVector.super.iterator(fromIndexInclusive, toIndexExclusive);
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
    public String toString() {
        return DoubleVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof DoubleVectorDirect) {
            return Arrays.equals(data, ((DoubleVectorDirect) obj).data);
        }
        return DoubleVector.equals(this, obj);
    }

    @Override
    public int hashCode() {
        return DoubleVector.hashCode(this);
    }
}
