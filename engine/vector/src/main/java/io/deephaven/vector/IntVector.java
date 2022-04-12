/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface IntVector extends Vector<IntVector> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<IntVector, Integer> type() {
        return PrimitiveVectorType.of(IntVector.class, IntType.instance());
    }

    int get(long i);

    @Override
    IntVector subVector(long fromIndex, long toIndex);

    @Override
    IntVector subVectorByPositions(long [] positions);

    @Override
    int[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class getComponentType() {
        return int.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory.  */
    @Override
    IntVector getDirect();

    static String intValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveIntValToString((Integer)val);
    }

    static String primitiveIntValToString(final int val) {
        return val == QueryConstants.NULL_INT ? NULL_ELEMENT_STRING : Integer.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The IntVector to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final IntVector array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveIntValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveIntValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a IntVector)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final IntVector aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof IntVector)) {
            return false;
        }
        final IntVector bArray = (IntVector) b;
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
     * @param array The IntVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final IntVector array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Integer.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" IntVector implementations.
     */
    abstract class Indirect implements IntVector {

        private static final long serialVersionUID = 1L;

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
            return new IntVectorDirect(toArray());
        }
    }
}
