/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface ShortVector extends Vector<ShortVector> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<ShortVector, Short> type() {
        return PrimitiveVectorType.of(ShortVector.class, ShortType.instance());
    }

    short get(long i);

    @Override
    ShortVector subVector(long fromIndex, long toIndex);

    @Override
    ShortVector subVectorByPositions(long [] positions);

    @Override
    short[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class getComponentType() {
        return short.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory.  */
    @Override
    ShortVector getDirect();

    static String shortValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveShortValToString((Short)val);
    }

    static String primitiveShortValToString(final short val) {
        return val == QueryConstants.NULL_SHORT ? NULL_ELEMENT_STRING : Short.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The ShortVector to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final ShortVector array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveShortValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveShortValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a ShortVector)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final ShortVector aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof ShortVector)) {
            return false;
        }
        final ShortVector bArray = (ShortVector) b;
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
     * @param array The ShortVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final ShortVector array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Short.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" ShortVector implementations.
     */
    abstract class Indirect implements ShortVector {

        private static final long serialVersionUID = 1L;

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

        protected final Object writeReplace() {
            return new ShortVectorDirect(toArray());
        }
    }
}
