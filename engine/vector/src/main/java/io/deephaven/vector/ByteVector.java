//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfByte;
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
public interface ByteVector extends Vector<ByteVector>, Iterable<Byte> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<ByteVector, Byte> type() {
        return PrimitiveVectorType.of(ByteVector.class, ByteType.of());
    }

    /**
     * Get the element of this ByteVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_BYTE null byte}.
     *
     * @param index An offset into this ByteVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_BYTE null byte}
     */
    byte get(long index);

    @Override
    ByteVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    ByteVector subVectorByPositions(long[] positions);

    @Override
    byte[] toArray();

    @Override
    byte[] copyToArray();

    @Override
    ByteVector getDirect();

    @Override
    @FinalDefault
    default ValueIteratorOfByte iterator() {
        return iterator(0, size());
    }

    /**
     * Returns an iterator over a slice of this vector, with equivalent semantics to
     * {@code subVector(fromIndexInclusive, toIndexExclusive).iterator()}.
     *
     * @param fromIndexInclusive The first position to include
     * @param toIndexExclusive The first position after {@code fromIndexInclusive} to not include
     * @return An iterator over the requested slice
     */
    default ValueIteratorOfByte iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new ValueIteratorOfByte() {

            long nextIndex = fromIndexInclusive;

            @Override
            public byte nextByte() {
                return get(nextIndex++);
            }

            @Override
            public boolean hasNext() {
                return nextIndex < toIndexExclusive;
            }

            @Override
            public long remaining() {
                return toIndexExclusive - nextIndex;
            }
        };
    }

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
        try (final CloseablePrimitiveIteratorOfByte iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveByteValToString(iterator.nextByte()));
            iterator.forEachRemaining(
                    (final byte value) -> builder.append(',').append(primitiveByteValToString(value)));
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
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfByte aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfByte bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (aIterator.nextByte() != bIterator.nextByte()) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(byte[])}.
     *
     * @param vector The ByteVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final ByteVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfByte iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Byte.hashCode(iterator.nextByte());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" ByteVector implementations.
     */
    abstract class Indirect implements ByteVector {

        @Override
        public byte[] toArray() {
            final int size = intSize("ByteVector.toArray");
            final byte[] result = new byte[size];
            try (final CloseablePrimitiveIteratorOfByte iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextByte();
                }
            }
            return result;
        }

        @Override
        public byte[] copyToArray() {
            return toArray();
        }

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
            return getDirect();
        }
    }
}
