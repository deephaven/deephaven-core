/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface ByteVector extends Vector<ByteVector> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<ByteVector, Byte> type() {
        return PrimitiveVectorType.of(ByteVector.class, ByteType.instance());
    }

    byte get(long i);

    @Override
    ByteVector subVector(long fromIndex, long toIndex);

    @Override
    ByteVector subVectorByPositions(long [] positions);

    @Override
    byte[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class getComponentType() {
        return byte.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory.  */
    @Override
    ByteVector getDirect();

    static String byteValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveByteValToString((Byte)val);
    }

    static String primitiveByteValToString(final byte val) {
        return val == QueryConstants.NULL_BYTE ? NULL_ELEMENT_STRING : Byte.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The ByteVector to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final ByteVector array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveByteValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveByteValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a ByteVector)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final ByteVector aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof ByteVector)) {
            return false;
        }
        final ByteVector bArray = (ByteVector) b;
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
     * @param array The ByteVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final ByteVector array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Byte.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" ByteVector implementations.
     */
    abstract class Indirect implements ByteVector {

        private static final long serialVersionUID = 1L;

        @Override
        public ByteVector getDirect() {
            return new ByteVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return ByteVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return ByteVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return ByteVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return new ByteVectorDirect(toArray());
        }
    }
}
