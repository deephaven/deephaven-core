/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

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
public interface IntVector extends Vector<IntVector> {

    static PrimitiveVectorType<IntVector, Integer> type() {
        return PrimitiveVectorType.of(IntVector.class, IntType.instance());
    }

    int get(long index);

    @Override
    IntVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    IntVector subVectorByPositions(long[] positions);

    @Override
    int[] toArray();

    @Override
    long size();

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

    /** Return a version of this Vector that is flattened out to only reference memory. */
    @Override
    IntVector getDirect();

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
        builder.append(primitiveIntValToString(vector.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveIntValToString(vector.get(ei)));
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
        for (long ei = 0; ei < size; ++ei) {
            // region elementEquals
            if (aVector.get(ei) != bVector.get(ei)) {
                return false;
            }
            // endregion elementEquals
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern
     * in {@link Arrays#hashCode(int[])}.
     *
     * @param vector The IntVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final IntVector vector) {
        final long size = vector.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Integer.hashCode(vector.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" IntVector implementations.
     */
    abstract class Indirect implements IntVector {

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
    }
}
