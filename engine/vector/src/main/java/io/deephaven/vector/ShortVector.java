/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

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
public interface ShortVector extends Vector<ShortVector> {

    static PrimitiveVectorType<ShortVector, Short> type() {
        return PrimitiveVectorType.of(ShortVector.class, ShortType.instance());
    }

    short get(long index);

    @Override
    ShortVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    ShortVector subVectorByPositions(long[] positions);

    @Override
    short[] toArray();

    @Override
    long size();

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

    /** Return a version of this Vector that is flattened out to only reference memory. */
    @Override
    ShortVector getDirect();

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
        builder.append(primitiveShortValToString(vector.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveShortValToString(vector.get(ei)));
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
     * in {@link Arrays#hashCode(short[])}.
     *
     * @param vector The ShortVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final ShortVector vector) {
        final long size = vector.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Short.hashCode(vector.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" ShortVector implementations.
     */
    abstract class Indirect implements ShortVector {

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
    }
}
