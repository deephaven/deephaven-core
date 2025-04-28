//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorDirect and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfShort;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * A {@link ShortVector} backed by an array.
 */
@ArrayType(type = short[].class)
public final class ShortVectorDirect implements ShortVector {

    private final static long serialVersionUID = 3636374971797603565L;

    public static final ShortVector ZERO_LENGTH_VECTOR = new ShortVectorDirect();

    private final short[] data;

    public ShortVectorDirect(@NotNull final short... data) {
        this.data = Require.neqNull(data, "data");
    }

    @Override
    public short get(final long index) {
        if (index < 0 || index >= data.length) {
            return NULL_SHORT;
        }
        return data[(int) index];
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
    public short[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    @Override
    public ValueIteratorOfShort iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return ValueIteratorOfShort.of(data);
        }
        return ShortVector.super.iterator(fromIndexInclusive, toIndexExclusive);
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
    public String toString() {
        return ShortVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ShortVectorDirect) {
            return Arrays.equals(data, ((ShortVectorDirect) obj).data);
        }
        return ShortVector.equals(this, obj);
    }

    @Override
    public int hashCode() {
        return ShortVector.hashCode(this);
    }
}
