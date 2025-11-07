//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorDirect and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfLong;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * A {@link LongVector} backed by an array.
 */
@ArrayType(type = long[].class)
public final class LongVectorDirect implements LongVector {

    private final static long serialVersionUID = 3636374971797603565L;

    public static final LongVector ZERO_LENGTH_VECTOR = new LongVectorDirect();

    private final long[] data;

    public LongVectorDirect(@NotNull final long... data) {
        this.data = Require.neqNull(data, "data");
    }

    @Override
    public long get(final long index) {
        if (index < 0 || index >= data.length) {
            return NULL_LONG;
        }
        return data[(int) index];
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
    public long[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    @Override
    public ValueIteratorOfLong iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return ValueIteratorOfLong.of(data);
        }
        return LongVector.super.iterator(fromIndexInclusive, toIndexExclusive);
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
    public String toString() {
        return LongVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof LongVectorDirect) {
            return Arrays.equals(data, ((LongVectorDirect) obj).data);
        }
        return LongVector.equals(this, obj);
    }

    // region compareTo
    @Override
    public int compareTo(final LongVector o) {
        if (o instanceof LongVectorDirect) {
            // The byte, short, integer, and long versions can use direct vector comparisons as our order matches the
            // natural order of numbers.
            //
            // Float and double can not use direct vector comparisons. Although NaN works because Float.compare and
            // Double.compare sort NaNs last, as we do for Deephaven values; and the Arrays.compare is defined to work
            // as Float.compare. However, Float.compare(0f,-0) does not produce 0 but rather ranks -0 as less than 0.
            return Arrays.compare(data, ((LongVectorDirect) o).data);
        }
        return LongVector.super.compareTo(o);
    }

    // endregion compareTo

    @Override
    public int hashCode() {
        return LongVector.hashCode(this);
    }
}
