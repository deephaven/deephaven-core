//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.qst.type.IntType;
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
 * A {@link Vector} of primitive ints.
 */
public interface IntVector extends Vector<IntVector>, Iterable<Integer> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<IntVector, Integer> type() {
        return PrimitiveVectorType.of(IntVector.class, IntType.of());
    }

    /**
     * Get the element of this IntVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_INT null int}.
     *
     * @param index An offset into this IntVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_INT null int}
     */
    int get(long index);

    @Override
    IntVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    IntVector subVectorByPositions(long[] positions);

    @Override
    int[] toArray();

    @Override
    int[] copyToArray();

    @Override
    IntVector getDirect();

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
            public int nextInt() {
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
        return int.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String intValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveIntValToString((Integer) val);
    }

    static String primitiveIntValToString(final int val) {
        return val == QueryConstants.NULL_INT ? NULL_ELEMENT_STRING : Integer.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The IntVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final IntVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfInt iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveIntValToString(iterator.nextInt()));
            iterator.forEachRemaining(
                    (final int value) -> builder.append(',').append(primitiveIntValToString(value)));
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
     * @param aVector The LHS of the equality test (always a IntVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final IntVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof IntVector)) {
            return false;
        }
        final IntVector bVector = (IntVector) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfInt aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfInt bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (aIterator.nextInt() != bIterator.nextInt()) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(int[])}.
     *
     * @param vector The IntVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final IntVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfInt iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Integer.hashCode(iterator.nextInt());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" IntVector implementations.
     */
    abstract class Indirect implements IntVector {

        @Override
        public int[] toArray() {
            final int size = intSize("IntVector.toArray");
            final int[] result = new int[size];
            try (final CloseablePrimitiveIteratorOfInt iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextInt();
                }
            }
            return result;
        }

        @Override
        public int[] copyToArray() {
            return toArray();
        }

        @Override
        public IntVector getDirect() {
            return new IntVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return IntVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return IntVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return IntVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }

    interface Iterator extends CloseablePrimitiveIteratorOfInt {
        @Override
        @FinalDefault
        default Integer next() {
            return TypeUtils.box(nextInt());
        }

        @Override
        @FinalDefault
        default void forEachRemaining(@NotNull final Consumer<? super Integer> action) {
            forEachRemaining((final int element) -> action.accept(TypeUtils.box(element)));
        }

        /**
         * A re-usable, immutable IntegerColumnIterator with no elements.
         */
        Iterator EMPTY = new Iterator() {
            @Override
            public int nextInt() {
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return false;
            }
        };

        // region streamAsInt
        
        /**
        * Create a boxed {@link Stream} over the remaining elements of this IntegerColumnIterator. The result <em>must</em>
        * be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
        * try-with-resources block is strongly encouraged.
        *
        * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()
        *         closed}.
        */
        @Override
        @FinalDefault
        default Stream<Integer> stream() {
           return intStream().mapToObj(TypeUtils::box);
        }
        // endregion streamAsInt

        /**
         * Get a IntegerColumnIterator with no elements. The result does not need to be {@link #close() closed}.
         *
         * @return A IntegerColumnIterator with no elements
         */
        static Iterator empty() {
            return EMPTY;
        }

        /**
         * Create a IntegerColumnIterator over an array of {@code int}. The result does not need to be
         * {@link #close() closed}.
         *
         * @param values The elements to iterate
         * @return A IntegerColumnIterator of {@code values}
         */
        static Iterator of(@NotNull final int... values) {
            Objects.requireNonNull(values);
            return new Iterator() {

                private int valueIndex;

                @Override
                public int nextInt() {
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
         * Create a IntegerColumnIterator that repeats {@code value}, {@code repeatCount} times. The result does not
         * need to be {@link #close() closed}.
         *
         * @param value The value to repeat
         * @param repeatCount The number of repetitions
         * @return A IntegerColumnIterator that repeats {@code value}, {@code repeatCount} times
         */
        static Iterator repeat(final int value, final long repeatCount) {
            return new Iterator() {

                private long repeatIndex;

                @Override
                public int nextInt() {
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
         * Create a IntegerColumnIterator that concatenates an array of non-{@code null} {@code subIterators}. The
         * result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
         *
         * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing
         *        an array, ensure that this iterator has full ownership.
         * @return A IntegerColumnIterator concatenating all elements from {@code subIterators}
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
                public int nextInt() {
                    if (hasNext()) {
                        hasNextChecked = false;
                        return subIterators[subIteratorIndex].nextInt();
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
         * Return a IntegerColumnIterator that concatenates the contents of any non-{@code null}
         * IntegerColumnIterator found amongst {@code first}, {@code second}, and {@code third}.
         *
         * @param first The first iterator to consider concatenating
         * @param second The second iterator to consider concatenating
         * @param third The third iterator to consider concatenating
         * @return A IntegerColumnIterator that concatenates all elements as specified
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
