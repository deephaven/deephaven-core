/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

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
public interface FloatVector extends Vector<FloatVector> {

    static PrimitiveVectorType<FloatVector, Float> type() {
        return PrimitiveVectorType.of(FloatVector.class, FloatType.instance());
    }

    float get(long index);

    @Override
    FloatVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    FloatVector subVectorByPositions(long[] positions);

    @Override
    float[] toArray();

    @Override
    long size();

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

    /** Return a version of this Vector that is flattened out to only reference memory. */
    @Override
    FloatVector getDirect();

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
        builder.append(primitiveFloatValToString(vector.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveFloatValToString(vector.get(ei)));
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
     * in {@link Arrays#hashCode(float[])}.
     *
     * @param vector The FloatVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final FloatVector vector) {
        final long size = vector.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Float.hashCode(vector.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" FloatVector implementations.
     */
    abstract class Indirect implements FloatVector {

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
    }
}
