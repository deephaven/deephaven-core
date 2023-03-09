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

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * A {@link IntVector} backed by an array.
 */
@ArrayType(type = int[].class)
public class IntVectorDirect implements IntVector {

    public static final IntVector ZERO_LENGTH_VECTOR = new IntVectorDirect();

    private final int[] data;

    public IntVectorDirect(final int... data) {
        this.data = data;
    }

    @Override
    public int get(final long index) {
        if (index < 0 || index > data.length - 1) {
            return NULL_INT;
        }
        return data[LongSizedDataStructure.intSize("IntVectorDirect get", index)];
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
    public long size() {
        return data.length;
    }

    @Override
    public IntVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return IntVector.toString(this, 10);
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj instanceof IntVectorDirect) {
            return Arrays.equals(data, ((IntVectorDirect) obj).data);
        }
        return IntVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return IntVector.hashCode(this);
    }
}
