/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * A {@link CharVector} backed by an array.
 */
@ArrayType(type = char[].class)
public class CharVectorDirect implements CharVector {

    public static final CharVector ZERO_LENGTH_VECTOR = new CharVectorDirect();

    private final char[] data;

    public CharVectorDirect(final char... data) {
        this.data = data;
    }

    @Override
    public char get(final long index) {
        if (index < 0 || index > data.length - 1) {
            return NULL_CHAR;
        }
        return data[LongSizedDataStructure.intSize("CharVectorDirect get", index)];
    }

    @Override
    public CharVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new CharVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public CharVector subVectorByPositions(final long[] positions) {
        return new CharSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public char[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public CharVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return CharVector.toString(this, 10);
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj instanceof CharVectorDirect) {
            return Arrays.equals(data, ((CharVectorDirect) obj).data);
        }
        return CharVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return CharVector.hashCode(this);
    }
}
