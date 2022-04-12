/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface FloatVector extends Vector<FloatVector> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<FloatVector, Float> type() {
        return PrimitiveVectorType.of(FloatVector.class, FloatType.instance());
    }

    float get(long i);

    @Override
    FloatVector subVector(long fromIndex, long toIndex);

    @Override
    FloatVector subVectorByPositions(long [] positions);

    @Override
    float[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class getComponentType() {
        return float.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory.  */
    @Override
    FloatVector getDirect();

    static String floatValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveFloatValToString((Float)val);
    }

    static String primitiveFloatValToString(final float val) {
        return val == QueryConstants.NULL_FLOAT ? NULL_ELEMENT_STRING : Float.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The FloatVector to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final FloatVector array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveFloatValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveFloatValToString(array.get(ei)));
        }
        if (displaySize == array.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link Object#equals(Object)}.
     *
     * @param aArray The LHS of the equality test (always a FloatVector)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final FloatVector aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof FloatVector)) {
            return false;
        }
        final FloatVector bArray = (FloatVector) b;
        final long size = aArray.size();
        if (size != bArray.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            // region elementEquals
            if (Float.floatToIntBits(aArray.get(ei)) != Float.floatToIntBits(bArray.get(ei))) {
                return false;
            }
            // endregion elementEquals
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in
     * {@link Arrays#hashCode(Object[])}.
     *
     * @param array The FloatVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final FloatVector array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Float.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" FloatVector implementations.
     */
    abstract class Indirect implements FloatVector {

        private static final long serialVersionUID = 1L;

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
            return new FloatVectorDirect(toArray());
        }
    }
}
