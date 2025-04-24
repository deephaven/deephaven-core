//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.function.CharToIntFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfChar;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    default Iterator iterator() {
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
    default Iterator iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new Iterator() {

            long nextIndex = fromIndexInclusive;

            @Override
            public char nextChar() {
                return get(nextIndex++);
            }

            @Override
            public boolean hasNext() {
                return nextIndex < toIndexExclusive;
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

    interface Iterator extends CloseablePrimitiveIteratorOfChar {
        @Override
        @FinalDefault
        default Character next() {
            return TypeUtils.box(nextChar());
        }

        @Override
        @FinalDefault
        default void forEachRemaining(@NotNull final Consumer<? super Character> action) {
            forEachRemaining((final char element) -> action.accept(TypeUtils.box(element)));
        }

        /**
         * A re-usable, immutable CharacterColumnIterator with no elements.
         */
        Iterator EMPTY = new Iterator() {
            @Override
            public char nextChar() {
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return false;
            }
        };

        // region streamAsInt
        /**
         * Create an unboxed {@link IntStream} over the remaining elements of this ChunkedCharacterColumnIterator by
         * casting each element to {@code int} with the appropriate adjustment of
         * {@link io.deephaven.util.QueryConstants#NULL_CHAR NULL_CHAR} to
         * {@link io.deephaven.util.QueryConstants#NULL_INT NULL_INT}. The result <em>must</em> be
         * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
         * try-with-resources block is strongly encouraged.
         *
         * @return An unboxed {@link IntStream} over the remaining contents of this iterator. Must be
         *         {@link Stream#close() closed}.
         */
        @Override
        @FinalDefault
        default IntStream streamAsInt() {
            return streamAsInt(
                    (final char value) -> value == QueryConstants.NULL_CHAR ? QueryConstants.NULL_INT : (int) value);
        }
        // endregion streamAsInt

        /**
         * Get a CharacterColumnIterator with no elements. The result does not need to be {@link #close() closed}.
         *
         * @return A CharacterColumnIterator with no elements
         */
        static Iterator empty() {
            return EMPTY;
        }

        /**
         * Create a CharacterColumnIterator over an array of {@code char}. The result does not need to be
         * {@link #close() closed}.
         *
         * @param values The elements to iterate
         * @return A CharacterColumnIterator of {@code values}
         */
        static Iterator of(@NotNull final char... values) {
            Objects.requireNonNull(values);
            return new Iterator() {

                private int valueIndex;

                @Override
                public char nextChar() {
                    if (valueIndex < values.length) {
                        return values[valueIndex++];
                    }
                    throw new NoSuchElementException();
                }

                @Override
                public boolean hasNext() {
                    return valueIndex < values.length;
                }
            };
        }

        /**
         * Create a CharacterColumnIterator that repeats {@code value}, {@code repeatCount} times. The result does not
         * need to be {@link #close() closed}.
         *
         * @param value The value to repeat
         * @param repeatCount The number of repetitions
         * @return A CharacterColumnIterator that repeats {@code value}, {@code repeatCount} times
         */
        static Iterator repeat(final char value, final long repeatCount) {
            return new Iterator() {

                private long repeatIndex;

                @Override
                public char nextChar() {
                    if (repeatIndex < repeatCount) {
                        ++repeatIndex;
                        return value;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                public boolean hasNext() {
                    return repeatIndex < repeatCount;
                }
            };
        }

        /**
         * Create a CharacterColumnIterator that concatenates an array of non-{@code null} {@code subIterators}. The
         * result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
         *
         * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing
         *        an array, ensure that this iterator has full ownership.
         * @return A CharacterColumnIterator concatenating all elements from {@code subIterators}
         */
        static Iterator concat(@NotNull final Iterator... subIterators) {
            Objects.requireNonNull(subIterators);
            if (subIterators.length == 0) {
                return empty();
            }
            if (subIterators.length == 1) {
                return subIterators[0];
            }
            return new Iterator() {

                private boolean hasNextChecked;
                private int subIteratorIndex;

                @Override
                public char nextChar() {
                    if (hasNext()) {
                        hasNextChecked = false;
                        return subIterators[subIteratorIndex].nextChar();
                    }
                    throw new NoSuchElementException();
                }

                @Override
                public boolean hasNext() {
                    if (hasNextChecked) {
                        return true;
                    }
                    for (; subIteratorIndex < subIterators.length; ++subIteratorIndex) {
                        if (subIterators[subIteratorIndex].hasNext()) {
                            return hasNextChecked = true;
                        }
                    }
                    return false;
                }

                @Override
                public void close() {
                    SafeCloseableArray.close(subIterators);
                }
            };
        }

        /**
         * Return a CharacterColumnIterator that concatenates the contents of any non-{@code null}
         * CharacterColumnIterator found amongst {@code first}, {@code second}, and {@code third}.
         *
         * @param first The first iterator to consider concatenating
         * @param second The second iterator to consider concatenating
         * @param third The third iterator to consider concatenating
         * @return A CharacterColumnIterator that concatenates all elements as specified
         */
        static Iterator maybeConcat(
                @Nullable final Iterator first,
                @Nullable final Iterator second,
                @Nullable final Iterator third) {
            if (first != null) {
                if (second != null) {
                    if (third != null) {
                        return concat(first, second, third);
                    }
                    return concat(first, second);
                }
                if (third != null) {
                    return concat(first, third);
                }
                return first;
            }
            if (second != null) {
                if (third != null) {
                    return concat(second, third);
                }
                return second;
            }
            if (third != null) {
                return third;
            }
            return empty();
        }
    }
}
