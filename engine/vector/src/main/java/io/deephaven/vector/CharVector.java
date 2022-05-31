/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface CharVector extends Vector<CharVector> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<CharVector, Character> type() {
        return PrimitiveVectorType.of(CharVector.class, CharType.instance());
    }

    char get(long i);

    @Override
    CharVector subVector(long fromIndex, long toIndex);

    @Override
    CharVector subVectorByPositions(long [] positions);

    @Override
    char[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class getComponentType() {
        return char.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory.  */
    @Override
    CharVector getDirect();

    static String charValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveCharValToString((Character)val);
    }

    static String primitiveCharValToString(final char val) {
        return val == QueryConstants.NULL_CHAR ? NULL_ELEMENT_STRING : Character.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The CharVector to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final CharVector array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveCharValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveCharValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a CharVector)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final CharVector aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof CharVector)) {
            return false;
        }
        final CharVector bArray = (CharVector) b;
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
     * @param array The CharVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final CharVector array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Character.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" CharVector implementations.
     */
    abstract class Indirect implements CharVector {

        private static final long serialVersionUID = 1L;

        @Override
        public CharVector getDirect() {
            return new CharVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return CharVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return CharVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return CharVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return new CharVectorDirect(toArray());
        }
    }
}
