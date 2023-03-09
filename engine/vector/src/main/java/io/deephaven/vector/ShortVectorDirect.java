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

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * A {@link ShortVector} backed by an array.
 */
@ArrayType(type = short[].class)
public class ShortVectorDirect implements ShortVector {

    public static final ShortVector ZERO_LENGTH_VECTOR = new ShortVectorDirect();

    private final short[] data;

    public ShortVectorDirect(final short... data) {
        this.data = data;
    }

    @Override
    public short get(final long index) {
        if (index < 0 || index > data.length - 1) {
            return NULL_SHORT;
        }
        return data[LongSizedDataStructure.intSize("ShortVectorDirect get", index)];
    }

    @Override
    public ShortVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new ShortVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public ShortVector subVectorByPositions(final long[] positions) {
        return new ShortSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public short[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public ShortVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return ShortVector.toString(this, 10);
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj instanceof ShortVectorDirect) {
            return Arrays.equals(data, ((ShortVectorDirect) obj).data);
        }
        return ShortVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return ShortVector.hashCode(this);
    }
}
