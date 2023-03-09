/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * A {@link Vector} of primitive chars.
 */
public interface CharVector extends Vector<CharVector> {

    static PrimitiveVectorType<CharVector, Character> type() {
        return PrimitiveVectorType.of(CharVector.class, CharType.instance());
    }

    char get(long index);

    @Override
    CharVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    CharVector subVectorByPositions(long[] positions);

    @Override
    char[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class<?> getComponentType() {
        return char.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory. */
    @Override
    CharVector getDirect();

    static String charValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveCharValToString((Character) val);
    }

    static String primitiveCharValToString(final char val) {
        return val == QueryConstants.NULL_CHAR ? NULL_ELEMENT_STRING : Character.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The CharVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final CharVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        builder.append(primitiveCharValToString(vector.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveCharValToString(vector.get(ei)));
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
     * @param aVector The LHS of the equality test (always a CharVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final CharVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof CharVector)) {
            return false;
        }
        final CharVector bVector = (CharVector) bObj;
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
     * in {@link Arrays#hashCode(char[])}.
     *
     * @param vector The CharVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final CharVector vector) {
        final long size = vector.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Character.hashCode(vector.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" CharVector implementations.
     */
    abstract class Indirect implements CharVector {

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
    }
}
