//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorDirect and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfInt;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * A {@link IntVector} backed by an array.
 */
@ArrayType(type = int[].class)
public final class IntVectorDirect implements IntVector {

    private final static long serialVersionUID = 3636374971797603565L;

    public static final IntVector ZERO_LENGTH_VECTOR = new IntVectorDirect();

    private final int[] data;

    public IntVectorDirect(@NotNull final int... data) {
        this.data = Require.neqNull(data, "data");
    }

    @Override
    public int get(final long index) {
        if (index < 0 || index >= data.length) {
            return NULL_INT;
        }
        return data[(int) index];
    }

    @Override
    public IntVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new IntVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public IntVector subVectorByPositions(final long[] positions) {
        return new IntSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public int[] toArray() {
        return data;
    }

    @Override
    public int[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    @Override
    public ValueIteratorOfInt iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return ValueIteratorOfInt.of(data);
        }
        return IntVector.super.iterator(fromIndexInclusive, toIndexExclusive);
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public IntVectorDirect getDirect() {
        return this;
    }

    @Override
    public String toString() {
        return IntVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof IntVectorDirect) {
            return Arrays.equals(data, ((IntVectorDirect) obj).data);
        }
        return IntVector.equals(this, obj);
    }

    @Override
    public int hashCode() {
        return IntVector.hashCode(this);
    }
}
