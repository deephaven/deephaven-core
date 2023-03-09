/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * A {@link Vector} of primitive longs.
 */
public interface LongVector extends Vector<LongVector> {

    static PrimitiveVectorType<LongVector, Long> type() {
        return PrimitiveVectorType.of(LongVector.class, LongType.instance());
    }

    long get(long index);

    @Override
    LongVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    LongVector subVectorByPositions(long[] positions);

    @Override
    long[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class<?> getComponentType() {
        return long.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory. */
    @Override
    LongVector getDirect();

    static String longValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveLongValToString((Long) val);
    }

    static String primitiveLongValToString(final long val) {
        return val == QueryConstants.NULL_LONG ? NULL_ELEMENT_STRING : Long.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The LongVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final LongVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        builder.append(primitiveLongValToString(vector.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveLongValToString(vector.get(ei)));
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
     * @param aVector The LHS of the equality test (always a LongVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final LongVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof LongVector)) {
            return false;
        }
        final LongVector bVector = (LongVector) bObj;
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
     * in {@link Arrays#hashCode(long[])}.
     *
     * @param vector The LongVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final LongVector vector) {
        final long size = vector.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Long.hashCode(vector.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" LongVector implementations.
     */
    abstract class Indirect implements LongVector {

        @Override
        public LongVector getDirect() {
            return new LongVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return LongVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return LongVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return LongVector.hashCode(this);
        }
    }
}
