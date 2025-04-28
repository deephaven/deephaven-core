//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorDirect and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfByte;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * A {@link ByteVector} backed by an array.
 */
@ArrayType(type = byte[].class)
public final class ByteVectorDirect implements ByteVector {

    private final static long serialVersionUID = 3636374971797603565L;

    public static final ByteVector ZERO_LENGTH_VECTOR = new ByteVectorDirect();

    private final byte[] data;

    public ByteVectorDirect(@NotNull final byte... data) {
        this.data = Require.neqNull(data, "data");
    }

    @Override
    public byte get(final long index) {
        if (index < 0 || index >= data.length) {
            return NULL_BYTE;
        }
        return data[(int) index];
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
    public byte[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    @Override
    public ValueIteratorOfByte iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return ValueIteratorOfByte.of(data);
        }
        return ByteVector.super.iterator(fromIndexInclusive, toIndexExclusive);
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
    public String toString() {
        return ByteVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ByteVectorDirect) {
            return Arrays.equals(data, ((ByteVectorDirect) obj).data);
        }
        return ByteVector.equals(this, obj);
    }

    @Override
    public int hashCode() {
        return ByteVector.hashCode(this);
    }
}
