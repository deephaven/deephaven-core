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

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * A {@link LongVector} backed by an array.
 */
@ArrayType(type = long[].class)
public class LongVectorDirect implements LongVector {

    public static final LongVector ZERO_LENGTH_VECTOR = new LongVectorDirect();

    private final long[] data;

    public LongVectorDirect(final long... data) {
        this.data = data;
    }

    @Override
    public long get(final long index) {
        if (index < 0 || index > data.length - 1) {
            return NULL_LONG;
        }
        return data[LongSizedDataStructure.intSize("LongVectorDirect get", index)];
    }

    @Override
    public LongVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new LongVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public LongVector subVectorByPositions(final long[] positions) {
        return new LongSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public long[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public LongVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return LongVector.toString(this, 10);
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj instanceof LongVectorDirect) {
            return Arrays.equals(data, ((LongVectorDirect) obj).data);
        }
        return LongVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return LongVector.hashCode(this);
    }
}
