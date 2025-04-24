//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.function.ByteToIntFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte;
import io.deephaven.qst.type.ByteType;
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
            public byte nextByte() {
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

    interface Iterator extends CloseablePrimitiveIteratorOfByte {
        @Override
        @FinalDefault
        default Byte next() {
            return TypeUtils.box(nextByte());
        }

        @Override
        @FinalDefault
        default void forEachRemaining(@NotNull final Consumer<? super Byte> action) {
            forEachRemaining((final byte element) -> action.accept(TypeUtils.box(element)));
        }

        /**
         * A re-usable, immutable ByteColumnIterator with no elements.
         */
        Iterator EMPTY = new Iterator() {
            @Override
            public byte nextByte() {
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return false;
            }
        };

        // region streamAsInt
        /**
         * Create an unboxed {@link IntStream} over the remaining elements of this ChunkedByteColumnIterator by casting
         * each element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_BYTE
         * NULL_BYTE} to {@link io.deephaven.util.QueryConstants#NULL_INT NULL_INT}. The result <em>must</em> be
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
                    (final byte value) -> value == QueryConstants.NULL_BYTE ? QueryConstants.NULL_INT : (int) value);
        }
        // endregion streamAsInt

        /**
         * Get a ByteColumnIterator with no elements. The result does not need to be {@link #close() closed}.
         *
         * @return A ByteColumnIterator with no elements
         */
        static Iterator empty() {
            return EMPTY;
        }

        /**
         * Create a ByteColumnIterator over an array of {@code byte}. The result does not need to be
         * {@link #close() closed}.
         *
         * @param values The elements to iterate
         * @return A ByteColumnIterator of {@code values}
         */
        static Iterator of(@NotNull final byte... values) {
            Objects.requireNonNull(values);
            return new Iterator() {

                private int valueIndex;

                @Override
                public byte nextByte() {
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
         * Create a ByteColumnIterator that repeats {@code value}, {@code repeatCount} times. The result does not
         * need to be {@link #close() closed}.
         *
         * @param value The value to repeat
         * @param repeatCount The number of repetitions
         * @return A ByteColumnIterator that repeats {@code value}, {@code repeatCount} times
         */
        static Iterator repeat(final byte value, final long repeatCount) {
            return new Iterator() {

                private long repeatIndex;

                @Override
                public byte nextByte() {
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
         * Create a ByteColumnIterator that concatenates an array of non-{@code null} {@code subIterators}. The
         * result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
         *
         * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing
         *        an array, ensure that this iterator has full ownership.
         * @return A ByteColumnIterator concatenating all elements from {@code subIterators}
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
                public byte nextByte() {
                    if (hasNext()) {
                        hasNextChecked = false;
                        return subIterators[subIteratorIndex].nextByte();
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
         * Return a ByteColumnIterator that concatenates the contents of any non-{@code null}
         * ByteColumnIterator found amongst {@code first}, {@code second}, and {@code third}.
         *
         * @param first The first iterator to consider concatenating
         * @param second The second iterator to consider concatenating
         * @param third The third iterator to consider concatenating
         * @return A ByteColumnIterator that concatenates all elements as specified
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
