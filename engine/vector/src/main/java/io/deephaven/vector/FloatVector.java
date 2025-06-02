//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfFloat;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfFloat;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * A {@link Vector} of primitive floats.
 */
public interface FloatVector extends Vector<FloatVector>, Iterable<Float> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<FloatVector, Float> type() {
        return PrimitiveVectorType.of(FloatVector.class, FloatType.of());
    }

    /**
     * Get the element of this FloatVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_FLOAT null float}.
     *
     * @param index An offset into this FloatVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_FLOAT null float}
     */
    float get(long index);

    @Override
    FloatVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    FloatVector subVectorByPositions(long[] positions);

    @Override
    float[] toArray();

    @Override
    float[] copyToArray();

    @Override
    FloatVector getDirect();

    @Override
    @FinalDefault
    default ValueIteratorOfFloat iterator() {
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
    default ValueIteratorOfFloat iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new ValueIteratorOfFloat() {

            long nextIndex = fromIndexInclusive;

            @Override
            public float nextFloat() {
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
        return float.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String floatValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveFloatValToString((Float) val);
    }

    static String primitiveFloatValToString(final float val) {
        return val == QueryConstants.NULL_FLOAT ? NULL_ELEMENT_STRING : Float.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The FloatVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final FloatVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfFloat iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveFloatValToString(iterator.nextFloat()));
            iterator.forEachRemaining(
                    (final float value) -> builder.append(',').append(primitiveFloatValToString(value)));
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
     * @param aVector The LHS of the equality test (always a FloatVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final FloatVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof FloatVector)) {
            return false;
        }
        final FloatVector bVector = (FloatVector) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfFloat aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfFloat bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (Float.floatToIntBits(aIterator.nextFloat()) != Float.floatToIntBits(bIterator.nextFloat())) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(float[])}.
     *
     * @param vector The FloatVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final FloatVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfFloat iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Float.hashCode(iterator.nextFloat());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" FloatVector implementations.
     */
    abstract class Indirect implements FloatVector {

        @Override
        public float[] toArray() {
            final int size = intSize("FloatVector.toArray");
            final float[] result = new float[size];
            try (final CloseablePrimitiveIteratorOfFloat iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextFloat();
                }
            }
            return result;
        }

        @Override
        public float[] copyToArray() {
            return toArray();
        }

        @Override
        public FloatVector getDirect() {
            return new FloatVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return FloatVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return FloatVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return FloatVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
