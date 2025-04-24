//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.function.ShortToIntFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfShort;
import io.deephaven.qst.type.ShortType;
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
 * A {@link Vector} of primitive shorts.
 */
public interface ShortVector extends Vector<ShortVector>, Iterable<Short> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<ShortVector, Short> type() {
        return PrimitiveVectorType.of(ShortVector.class, ShortType.of());
    }

    /**
     * Get the element of this ShortVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_SHORT null short}.
     *
     * @param index An offset into this ShortVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_SHORT null short}
     */
    short get(long index);

    @Override
    ShortVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    ShortVector subVectorByPositions(long[] positions);

    @Override
    short[] toArray();

    @Override
    short[] copyToArray();

    @Override
    ShortVector getDirect();

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
            public short nextShort() {
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
        return short.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String shortValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveShortValToString((Short) val);
    }

    static String primitiveShortValToString(final short val) {
        return val == QueryConstants.NULL_SHORT ? NULL_ELEMENT_STRING : Short.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The ShortVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final ShortVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfShort iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveShortValToString(iterator.nextShort()));
            iterator.forEachRemaining(
                    (final short value) -> builder.append(',').append(primitiveShortValToString(value)));
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
     * @param aVector The LHS of the equality test (always a ShortVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final ShortVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof ShortVector)) {
            return false;
        }
        final ShortVector bVector = (ShortVector) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfShort aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfShort bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (aIterator.nextShort() != bIterator.nextShort()) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(short[])}.
     *
     * @param vector The ShortVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final ShortVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfShort iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Short.hashCode(iterator.nextShort());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" ShortVector implementations.
     */
    abstract class Indirect implements ShortVector {

        @Override
        public short[] toArray() {
            final int size = intSize("ShortVector.toArray");
            final short[] result = new short[size];
            try (final CloseablePrimitiveIteratorOfShort iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextShort();
                }
            }
            return result;
        }

        @Override
        public short[] copyToArray() {
            return toArray();
        }

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
            return getDirect();
        }
    }

    interface Iterator extends CloseablePrimitiveIteratorOfShort {
        @Override
        @FinalDefault
        default Short next() {
            return TypeUtils.box(nextShort());
        }

        @Override
        @FinalDefault
        default void forEachRemaining(@NotNull final Consumer<? super Short> action) {
            forEachRemaining((final short element) -> action.accept(TypeUtils.box(element)));
        }

        /**
         * A re-usable, immutable ShortColumnIterator with no elements.
         */
        Iterator EMPTY = new Iterator() {
            @Override
            public short nextShort() {
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return false;
            }
        };

        // region streamAsInt
        /**
         * Create an unboxed {@link IntStream} over the remaining elements of this ChunkedShortColumnIterator by casting
         * each element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_SHORT
         * NULL_SHORT} to {@link io.deephaven.util.QueryConstants#NULL_INT NULL_INT}. The result <em>must</em> be
         * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
         * try-with-resources block is strongly encouraged.
         *
         * @return An unboxed {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close()
         *         closed}.
         */
        @Override
        @FinalDefault
        default IntStream streamAsInt() {
            return streamAsInt(
                    (final short value) -> value == QueryConstants.NULL_SHORT ? QueryConstants.NULL_INT : (int) value);
        }
        // endregion streamAsInt

        /**
         * Get a ShortColumnIterator with no elements. The result does not need to be {@link #close() closed}.
         *
         * @return A ShortColumnIterator with no elements
         */
        static Iterator empty() {
            return EMPTY;
        }

        /**
         * Create a ShortColumnIterator over an array of {@code short}. The result does not need to be
         * {@link #close() closed}.
         *
         * @param values The elements to iterate
         * @return A ShortColumnIterator of {@code values}
         */
        static Iterator of(@NotNull final short... values) {
            Objects.requireNonNull(values);
            return new Iterator() {

                private int valueIndex;

                @Override
                public short nextShort() {
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
         * Create a ShortColumnIterator that repeats {@code value}, {@code repeatCount} times. The result does not
         * need to be {@link #close() closed}.
         *
         * @param value The value to repeat
         * @param repeatCount The number of repetitions
         * @return A ShortColumnIterator that repeats {@code value}, {@code repeatCount} times
         */
        static Iterator repeat(final short value, final long repeatCount) {
            return new Iterator() {

                private long repeatIndex;

                @Override
                public short nextShort() {
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
         * Create a ShortColumnIterator that concatenates an array of non-{@code null} {@code subIterators}. The
         * result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
         *
         * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing
         *        an array, ensure that this iterator has full ownership.
         * @return A ShortColumnIterator concatenating all elements from {@code subIterators}
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
                public short nextShort() {
                    if (hasNext()) {
                        hasNextChecked = false;
                        return subIterators[subIteratorIndex].nextShort();
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
         * Return a ShortColumnIterator that concatenates the contents of any non-{@code null}
         * ShortColumnIterator found amongst {@code first}, {@code second}, and {@code third}.
         *
         * @param first The first iterator to consider concatenating
         * @param second The second iterator to consider concatenating
         * @param third The third iterator to consider concatenating
         * @return A ShortColumnIterator that concatenates all elements as specified
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
