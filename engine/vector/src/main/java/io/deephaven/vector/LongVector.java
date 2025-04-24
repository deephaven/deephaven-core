//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.qst.type.LongType;
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
 * A {@link Vector} of primitive longs.
 */
public interface LongVector extends Vector<LongVector>, Iterable<Long> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<LongVector, Long> type() {
        return PrimitiveVectorType.of(LongVector.class, LongType.of());
    }

    /**
     * Get the element of this LongVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_LONG null long}.
     *
     * @param index An offset into this LongVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_LONG null long}
     */
    long get(long index);

    @Override
    LongVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    LongVector subVectorByPositions(long[] positions);

    @Override
    long[] toArray();

    @Override
    long[] copyToArray();

    @Override
    LongVector getDirect();

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
            public long nextLong() {
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
        return long.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String longValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveLongValToString((Long) val);
    }

    static String primitiveLongValToString(final long val) {
        return val == QueryConstants.NULL_LONG ? NULL_ELEMENT_STRING : Long.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The LongVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final LongVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfLong iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveLongValToString(iterator.nextLong()));
            iterator.forEachRemaining(
                    (final long value) -> builder.append(',').append(primitiveLongValToString(value)));
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
     * @param aVector The LHS of the equality test (always a LongVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final LongVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof LongVector)) {
            return false;
        }
        final LongVector bVector = (LongVector) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfLong aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfLong bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (aIterator.nextLong() != bIterator.nextLong()) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(long[])}.
     *
     * @param vector The LongVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final LongVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfLong iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Long.hashCode(iterator.nextLong());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" LongVector implementations.
     */
    abstract class Indirect implements LongVector {

        @Override
        public long[] toArray() {
            final int size = intSize("LongVector.toArray");
            final long[] result = new long[size];
            try (final CloseablePrimitiveIteratorOfLong iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextLong();
                }
            }
            return result;
        }

        @Override
        public long[] copyToArray() {
            return toArray();
        }

        @Override
        public LongVector getDirect() {
            return new LongVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return LongVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return LongVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return LongVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }

    interface Iterator extends CloseablePrimitiveIteratorOfLong {
        @Override
        @FinalDefault
        default Long next() {
            return TypeUtils.box(nextLong());
        }

        @Override
        @FinalDefault
        default void forEachRemaining(@NotNull final Consumer<? super Long> action) {
            forEachRemaining((final long element) -> action.accept(TypeUtils.box(element)));
        }

        /**
         * A re-usable, immutable LongColumnIterator with no elements.
         */
        Iterator EMPTY = new Iterator() {
            @Override
            public long nextLong() {
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return false;
            }
        };

        // region streamAsInt
        // endregion streamAsInt

        /**
         * Get a LongColumnIterator with no elements. The result does not need to be {@link #close() closed}.
         *
         * @return A LongColumnIterator with no elements
         */
        static Iterator empty() {
            return EMPTY;
        }

        /**
         * Create a LongColumnIterator over an array of {@code long}. The result does not need to be
         * {@link #close() closed}.
         *
         * @param values The elements to iterate
         * @return A LongColumnIterator of {@code values}
         */
        static Iterator of(@NotNull final long... values) {
            Objects.requireNonNull(values);
            return new Iterator() {

                private int valueIndex;

                @Override
                public long nextLong() {
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
         * Create a LongColumnIterator that repeats {@code value}, {@code repeatCount} times. The result does not
         * need to be {@link #close() closed}.
         *
         * @param value The value to repeat
         * @param repeatCount The number of repetitions
         * @return A LongColumnIterator that repeats {@code value}, {@code repeatCount} times
         */
        static Iterator repeat(final long value, final long repeatCount) {
            return new Iterator() {

                private long repeatIndex;

                @Override
                public long nextLong() {
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
         * Create a LongColumnIterator that concatenates an array of non-{@code null} {@code subIterators}. The
         * result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
         *
         * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing
         *        an array, ensure that this iterator has full ownership.
         * @return A LongColumnIterator concatenating all elements from {@code subIterators}
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
                public long nextLong() {
                    if (hasNext()) {
                        hasNextChecked = false;
                        return subIterators[subIteratorIndex].nextLong();
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
         * Return a LongColumnIterator that concatenates the contents of any non-{@code null}
         * LongColumnIterator found amongst {@code first}, {@code second}, and {@code third}.
         *
         * @param first The first iterator to consider concatenating
         * @param second The second iterator to consider concatenating
         * @param third The third iterator to consider concatenating
         * @return A LongColumnIterator that concatenates all elements as specified
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
