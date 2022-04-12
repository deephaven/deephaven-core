/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface LongVector extends Vector<LongVector> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<LongVector, Long> type() {
        return PrimitiveVectorType.of(LongVector.class, LongType.instance());
    }

    long get(long i);

    @Override
    LongVector subVector(long fromIndex, long toIndex);

    @Override
    LongVector subVectorByPositions(long [] positions);

    @Override
    long[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class getComponentType() {
        return long.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory.  */
    @Override
    LongVector getDirect();

    static String longValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveLongValToString((Long)val);
    }

    static String primitiveLongValToString(final long val) {
        return val == QueryConstants.NULL_LONG ? NULL_ELEMENT_STRING : Long.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The LongVector to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final LongVector array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveLongValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveLongValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a LongVector)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final LongVector aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof LongVector)) {
            return false;
        }
        final LongVector bArray = (LongVector) b;
        final long size = aArray.size();
        if (size != bArray.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            // region elementEquals
            if (aArray.get(ei) != bArray.get(ei)) {
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
     * @param array The LongVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final LongVector array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Long.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" LongVector implementations.
     */
    abstract class Indirect implements LongVector {

        private static final long serialVersionUID = 1L;

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

        protected final Object writeReplace() {
            return new LongVectorDirect(toArray());
        }
    }
}
