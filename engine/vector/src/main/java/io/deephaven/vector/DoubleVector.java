//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.util.compare.DoubleComparisons;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfDouble;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfDouble;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.compare.DoubleComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * A {@link Vector} of primitive doubles.
 */
public interface DoubleVector extends Vector<DoubleVector>, Iterable<Double> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<DoubleVector, Double> type() {
        return PrimitiveVectorType.of(DoubleVector.class, DoubleType.of());
    }

    /**
     * Get the element of this DoubleVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_DOUBLE null double}.
     *
     * @param index An offset into this DoubleVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_DOUBLE null double}
     */
    double get(long index);

    @Override
    DoubleVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    DoubleVector subVectorByPositions(long[] positions);

    @Override
    double[] toArray();

    @Override
    double[] copyToArray();

    @Override
    DoubleVector getDirect();

    @Override
    @FinalDefault
    default ValueIteratorOfDouble iterator() {
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
    default ValueIteratorOfDouble iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new ValueIteratorOfDouble() {

            long nextIndex = fromIndexInclusive;

            @Override
            public double nextDouble() {
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
        return double.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /**
     * <p>
     * Compare this vector with another vector.
     * </p>
     *
     * <p>
     * The vectors are ordered lexicographically using Deephaven sorting rules.
     * </p>
     *
     * {@see Comparable#compareTo}
     */
    @Override
    default int compareTo(final DoubleVector o) {
        return compareTo(this, o);
    }

    static String doubleValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveDoubleValToString((Double) val);
    }

    static String primitiveDoubleValToString(final double val) {
        return val == QueryConstants.NULL_DOUBLE ? NULL_ELEMENT_STRING : Double.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The DoubleVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final DoubleVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfDouble iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveDoubleValToString(iterator.nextDouble()));
            iterator.forEachRemaining(
                    (final double value) -> builder.append(',').append(primitiveDoubleValToString(value)));
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
     * @param aVector The LHS of the equality test (always a DoubleVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DoubleVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof DoubleVector)) {
            return false;
        }
        final DoubleVector bVector = (DoubleVector) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfDouble aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfDouble bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (!DoubleComparisons.eq(aIterator.nextDouble(), bIterator.nextDouble())) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for {@link Comparable#compareTo(Object)} for a generic DoubleVector.
     * 
     * @param aVector the first vector (this in compareTo)
     * @param bVector the second vector ("o" or other in compareTo)
     * @return -1, 0, or 1 if aVector is less than, equal to, or greater than bVector (respectively)
     */
    static int compareTo(final DoubleVector aVector, final DoubleVector bVector) {
        if (aVector == bVector) {
            return 0;
        }
        try (final CloseablePrimitiveIteratorOfDouble aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfDouble bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                if (!bIterator.hasNext()) {
                    return 1;
                }
                final double aValue = aIterator.nextDouble();
                final double bValue = bIterator.nextDouble();
                final int compare = DoubleComparisons.compare(aValue, bValue);
                if (compare != 0) {
                    return compare;
                }
            }
            if (bIterator.hasNext()) {
                return -1;
            }
        }
        return 0;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(double[])}.
     *
     * @param vector The DoubleVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DoubleVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfDouble iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                // region ElementHash
                result = 31 * result + DoubleComparisons.hashCode(iterator.nextDouble());
                // endregion ElementHash
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" DoubleVector implementations.
     */
    abstract class Indirect implements DoubleVector {

        @Override
        public double[] toArray() {
            final int size = intSize("DoubleVector.toArray");
            final double[] result = new double[size];
            try (final CloseablePrimitiveIteratorOfDouble iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextDouble();
                }
            }
            return result;
        }

        @Override
        public double[] copyToArray() {
            return toArray();
        }

        @Override
        public DoubleVector getDirect() {
            return new DoubleVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return DoubleVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DoubleVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DoubleVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
