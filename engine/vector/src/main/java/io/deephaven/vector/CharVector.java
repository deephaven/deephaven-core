//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfChar;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfChar;
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
public interface CharVector extends Vector<CharVector>, Iterable<Character> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<CharVector, Character> type() {
        return PrimitiveVectorType.of(CharVector.class, CharType.of());
    }

    /**
     * Get the element of this CharVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_CHAR null char}.
     *
     * @param index An offset into this CharVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_CHAR null char}
     */
    char get(long index);

    @Override
    CharVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    CharVector subVectorByPositions(long[] positions);

    @Override
    char[] toArray();

    @Override
    char[] copyToArray();

    @Override
    CharVector getDirect();

    @Override
    @FinalDefault
    default ValueIteratorOfChar iterator() {
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
    default ValueIteratorOfChar iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new ValueIteratorOfChar() {

            long nextIndex = fromIndexInclusive;

            @Override
            public char nextChar() {
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
        return char.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

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
        try (final CloseablePrimitiveIteratorOfChar iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveCharValToString(iterator.nextChar()));
            iterator.forEachRemaining(
                    (final char value) -> builder.append(',').append(primitiveCharValToString(value)));
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
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfChar aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfChar bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (aIterator.nextChar() != bIterator.nextChar()) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(char[])}.
     *
     * @param vector The CharVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final CharVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfChar iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Character.hashCode(iterator.nextChar());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" CharVector implementations.
     */
    abstract class Indirect implements CharVector {

        @Override
        public char[] toArray() {
            final int size = intSize("CharVector.toArray");
            final char[] result = new char[size];
            try (final CloseablePrimitiveIteratorOfChar iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextChar();
                }
            }
            return result;
        }

        @Override
        public char[] copyToArray() {
            return toArray();
        }

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
            return getDirect();
        }
    }
}
