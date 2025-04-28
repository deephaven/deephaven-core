//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfShort;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfShort;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * A {@link Vector} of primitive shorts.
 */
public interface ShortVector extends Vector<ShortVector>, Iterable<Short> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<ShortVector, Short> type() {
        return PrimitiveVectorType.of(ShortVector.class, ShortType.of());
    }

    /**
     * Get the element of this ShortVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_SHORT null short}.
     *
     * @param index An offset into this ShortVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_SHORT null short}
     */
    short get(long index);

    @Override
    ShortVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    ShortVector subVectorByPositions(long[] positions);

    @Override
    short[] toArray();

    @Override
    short[] copyToArray();

    @Override
    ShortVector getDirect();

    @Override
    @FinalDefault
    default ValueIteratorOfShort iterator() {
        return iterator(0, size());
    }

    /**
     * Returns an iterator over a slice of this vector, with equivalent semantics to
     * {@code subVector(fromIndexInclusive, toIndexExclusive).iterator()}.
     *
     * @param fromIndexInclusive The first position to include
     * @param toIndexExclusive The first position after {@code fromIndexInclusive} to not include
     * @return An iterator over the requested slice
     */
    default ValueIteratorOfShort iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new ValueIteratorOfShort() {

            long nextIndex = fromIndexInclusive;

            @Override
            public short nextShort() {
                return get(nextIndex++);
            }

            @Override
            public boolean hasNext() {
                return nextIndex < toIndexExclusive;
            }

            @Override
            public long remaining() {
                return toIndexExclusive - nextIndex;
            }
        };
    }

    @Override
    @FinalDefault
    default Class<?> getComponentType() {
        return short.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String shortValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveShortValToString((Short) val);
    }

    static String primitiveShortValToString(final short val) {
        return val == QueryConstants.NULL_SHORT ? NULL_ELEMENT_STRING : Short.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The ShortVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final ShortVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfShort iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveShortValToString(iterator.nextShort()));
            iterator.forEachRemaining(
                    (final short value) -> builder.append(',').append(primitiveShortValToString(value)));
        }
        if (displaySize == vector.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link Object#equals(Object)}.
     *
     * @param aVector The LHS of the equality test (always a ShortVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final ShortVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof ShortVector)) {
            return false;
        }
        final ShortVector bVector = (ShortVector) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfShort aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfShort bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (aIterator.nextShort() != bIterator.nextShort()) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(short[])}.
     *
     * @param vector The ShortVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final ShortVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfShort iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Short.hashCode(iterator.nextShort());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" ShortVector implementations.
     */
    abstract class Indirect implements ShortVector {

        @Override
        public short[] toArray() {
            final int size = intSize("ShortVector.toArray");
            final short[] result = new short[size];
            try (final CloseablePrimitiveIteratorOfShort iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextShort();
                }
            }
            return result;
        }

        @Override
        public short[] copyToArray() {
            return toArray();
        }

        @Override
        public ShortVector getDirect() {
            return new ShortVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return ShortVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return ShortVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return ShortVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
