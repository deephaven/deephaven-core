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

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * A {@link ByteVector} backed by an array.
 */
@ArrayType(type = byte[].class)
public class ByteVectorDirect implements ByteVector {

    public static final ByteVector ZERO_LENGTH_VECTOR = new ByteVectorDirect();

    private final byte[] data;

    public ByteVectorDirect(final byte... data) {
        this.data = data;
    }

    @Override
    public byte get(final long index) {
        if (index < 0 || index > data.length - 1) {
            return NULL_BYTE;
        }
        return data[LongSizedDataStructure.intSize("ByteVectorDirect get", index)];
    }

    @Override
    public ByteVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new ByteVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public ByteVector subVectorByPositions(final long[] positions) {
        return new ByteSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public byte[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public ByteVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return ByteVector.toString(this, 10);
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj instanceof ByteVectorDirect) {
            return Arrays.equals(data, ((ByteVectorDirect) obj).data);
        }
        return ByteVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return ByteVector.hashCode(this);
    }
}
