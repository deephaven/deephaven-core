//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfChar;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * A {@link CharVector} backed by an array.
 */
@ArrayType(type = char[].class)
public final class CharVectorDirect implements CharVector {

    private final static long serialVersionUID = 3636374971797603565L;

    public static final CharVector ZERO_LENGTH_VECTOR = new CharVectorDirect();

    private final char[] data;

    public CharVectorDirect(@NotNull final char... data) {
        this.data = Require.neqNull(data, "data");
    }

    @Override
    public char get(final long index) {
        if (index < 0 || index >= data.length) {
            return NULL_CHAR;
        }
        return data[(int) index];
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
    public char[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    @Override
    public ValueIteratorOfChar iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return ValueIteratorOfChar.of(data);
        }
        return CharVector.super.iterator(fromIndexInclusive, toIndexExclusive);
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
    public String toString() {
        return CharVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof CharVectorDirect) {
            return Arrays.equals(data, ((CharVectorDirect) obj).data);
        }
        return CharVector.equals(this, obj);
    }

    @Override
    public int hashCode() {
        return CharVector.hashCode(this);
    }
}
