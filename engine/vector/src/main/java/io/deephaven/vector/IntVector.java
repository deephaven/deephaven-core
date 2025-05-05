//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfInt;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * A {@link Vector} of primitive ints.
 */
public interface IntVector extends Vector<IntVector>, Iterable<Integer> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<IntVector, Integer> type() {
        return PrimitiveVectorType.of(IntVector.class, IntType.of());
    }

    /**
     * Get the element of this IntVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_INT null int}.
     *
     * @param index An offset into this IntVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_INT null int}
     */
    int get(long index);

    @Override
    IntVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    IntVector subVectorByPositions(long[] positions);

    @Override
    int[] toArray();

    @Override
    int[] copyToArray();

    @Override
    IntVector getDirect();

    @Override
    @FinalDefault
    default ValueIteratorOfInt iterator() {
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
    default ValueIteratorOfInt iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new ValueIteratorOfInt() {

            long nextIndex = fromIndexInclusive;

            @Override
            public int nextInt() {
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
        return int.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String intValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveIntValToString((Integer) val);
    }

    static String primitiveIntValToString(final int val) {
        return val == QueryConstants.NULL_INT ? NULL_ELEMENT_STRING : Integer.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The IntVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final IntVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfInt iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveIntValToString(iterator.nextInt()));
            iterator.forEachRemaining(
                    (final int value) -> builder.append(',').append(primitiveIntValToString(value)));
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
     * @param aVector The LHS of the equality test (always a IntVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final IntVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof IntVector)) {
            return false;
        }
        final IntVector bVector = (IntVector) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfInt aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfInt bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (aIterator.nextInt() != bIterator.nextInt()) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(int[])}.
     *
     * @param vector The IntVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final IntVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfInt iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Integer.hashCode(iterator.nextInt());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" IntVector implementations.
     */
    abstract class Indirect implements IntVector {

        @Override
        public int[] toArray() {
            final int size = intSize("IntVector.toArray");
            final int[] result = new int[size];
            try (final CloseablePrimitiveIteratorOfInt iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextInt();
                }
            }
            return result;
        }

        @Override
        public int[] copyToArray() {
            return toArray();
        }

        @Override
        public IntVector getDirect() {
            return new IntVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return IntVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return IntVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return IntVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
