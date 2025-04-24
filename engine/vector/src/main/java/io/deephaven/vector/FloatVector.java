//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import java.util.stream.DoubleStream;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfFloat;
import io.deephaven.qst.type.FloatType;
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
 * A {@link Vector} of primitive floats.
 */
public interface FloatVector extends Vector<FloatVector>, Iterable<Float> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<FloatVector, Float> type() {
        return PrimitiveVectorType.of(FloatVector.class, FloatType.of());
    }

    /**
     * Get the element of this FloatVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_FLOAT null float}.
     *
     * @param index An offset into this FloatVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_FLOAT null float}
     */
    float get(long index);

    @Override
    FloatVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    FloatVector subVectorByPositions(long[] positions);

    @Override
    float[] toArray();

    @Override
    float[] copyToArray();

    @Override
    FloatVector getDirect();

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
            public float nextFloat() {
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
        return float.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String floatValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveFloatValToString((Float) val);
    }

    static String primitiveFloatValToString(final float val) {
        return val == QueryConstants.NULL_FLOAT ? NULL_ELEMENT_STRING : Float.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The FloatVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final FloatVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfFloat iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveFloatValToString(iterator.nextFloat()));
            iterator.forEachRemaining(
                    (final float value) -> builder.append(',').append(primitiveFloatValToString(value)));
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
     * @param aVector The LHS of the equality test (always a FloatVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final FloatVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof FloatVector)) {
            return false;
        }
        final FloatVector bVector = (FloatVector) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfFloat aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfFloat bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                // region ElementEquals
                if (Float.floatToIntBits(aIterator.nextFloat()) != Float.floatToIntBits(bIterator.nextFloat())) {
                    return false;
                }
                // endregion ElementEquals
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in {@link Arrays#hashCode(float[])}.
     *
     * @param vector The FloatVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final FloatVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfFloat iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Float.hashCode(iterator.nextFloat());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" FloatVector implementations.
     */
    abstract class Indirect implements FloatVector {

        @Override
        public float[] toArray() {
            final int size = intSize("FloatVector.toArray");
            final float[] result = new float[size];
            try (final CloseablePrimitiveIteratorOfFloat iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextFloat();
                }
            }
            return result;
        }

        @Override
        public float[] copyToArray() {
            return toArray();
        }

        @Override
        public FloatVector getDirect() {
            return new FloatVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return FloatVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return FloatVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return FloatVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }

    interface Iterator extends CloseablePrimitiveIteratorOfFloat {
        @Override
        @FinalDefault
        default Float next() {
            return TypeUtils.box(nextFloat());
        }

        @Override
        @FinalDefault
        default void forEachRemaining(@NotNull final Consumer<? super Float> action) {
            forEachRemaining((final float element) -> action.accept(TypeUtils.box(element)));
        }

        /**
         * A re-usable, immutable FloatColumnIterator with no elements.
         */
        Iterator EMPTY = new Iterator() {
            @Override
            public float nextFloat() {
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return false;
            }
        };

        // region streamAsInt
        /**
         * Create an unboxed {@link DoubleStream} over the remaining elements of this ChunkedFloatColumnIterator by casting
         * each element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_FLOAT
         * NULL_FLOAT} to {@link io.deephaven.util.QueryConstants#NULL_DOUBLE NULL_DOUBLE}. The result <em>must</em> be
         * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
         * try-with-resources block is strongly encouraged.
         *
         * @return An unboxed {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close()
         *         closed}.
         */
        @Override
        @FinalDefault
        default DoubleStream streamAsDouble() {
            return streamAsDouble(
                    (final float value) -> value == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_DOUBLE : (double) value);
        }
        // endregion streamAsInt

        /**
         * Get a FloatColumnIterator with no elements. The result does not need to be {@link #close() closed}.
         *
         * @return A FloatColumnIterator with no elements
         */
        static Iterator empty() {
            return EMPTY;
        }

        /**
         * Create a FloatColumnIterator over an array of {@code float}. The result does not need to be
         * {@link #close() closed}.
         *
         * @param values The elements to iterate
         * @return A FloatColumnIterator of {@code values}
         */
        static Iterator of(@NotNull final float... values) {
            Objects.requireNonNull(values);
            return new Iterator() {

                private int valueIndex;

                @Override
                public float nextFloat() {
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
         * Create a FloatColumnIterator that repeats {@code value}, {@code repeatCount} times. The result does not
         * need to be {@link #close() closed}.
         *
         * @param value The value to repeat
         * @param repeatCount The number of repetitions
         * @return A FloatColumnIterator that repeats {@code value}, {@code repeatCount} times
         */
        static Iterator repeat(final float value, final long repeatCount) {
            return new Iterator() {

                private long repeatIndex;

                @Override
                public float nextFloat() {
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
         * Create a FloatColumnIterator that concatenates an array of non-{@code null} {@code subIterators}. The
         * result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
         *
         * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing
         *        an array, ensure that this iterator has full ownership.
         * @return A FloatColumnIterator concatenating all elements from {@code subIterators}
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
                public float nextFloat() {
                    if (hasNext()) {
                        hasNextChecked = false;
                        return subIterators[subIteratorIndex].nextFloat();
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
         * Return a FloatColumnIterator that concatenates the contents of any non-{@code null}
         * FloatColumnIterator found amongst {@code first}, {@code second}, and {@code third}.
         *
         * @param first The first iterator to consider concatenating
         * @param second The second iterator to consider concatenating
         * @param third The third iterator to consider concatenating
         * @return A FloatColumnIterator that concatenates all elements as specified
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
