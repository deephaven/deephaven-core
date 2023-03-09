/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * A {@link Vector} of primitive bytes.
 */
public interface ByteVector extends Vector<ByteVector> {

    static PrimitiveVectorType<ByteVector, Byte> type() {
        return PrimitiveVectorType.of(ByteVector.class, ByteType.instance());
    }

    byte get(long index);

    @Override
    ByteVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    ByteVector subVectorByPositions(long[] positions);

    @Override
    byte[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class<?> getComponentType() {
        return byte.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory. */
    @Override
    ByteVector getDirect();

    static String byteValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveByteValToString((Byte) val);
    }

    static String primitiveByteValToString(final byte val) {
        return val == QueryConstants.NULL_BYTE ? NULL_ELEMENT_STRING : Byte.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The ByteVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final ByteVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        builder.append(primitiveByteValToString(vector.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveByteValToString(vector.get(ei)));
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
     * @param aVector The LHS of the equality test (always a ByteVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final ByteVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof ByteVector)) {
            return false;
        }
        final ByteVector bVector = (ByteVector) bObj;
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
     * in {@link Arrays#hashCode(byte[])}.
     *
     * @param vector The ByteVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final ByteVector vector) {
        final long size = vector.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Byte.hashCode(vector.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" ByteVector implementations.
     */
    abstract class Indirect implements ByteVector {

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
    }
}
