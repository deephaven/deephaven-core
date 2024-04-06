//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CloseablePrimitiveIteratorOfInt and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.iterator;

import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code long}.
 */
public interface CloseablePrimitiveIteratorOfLong
        extends PrimitiveIterator.OfLong, CloseablePrimitiveIterator<Long, LongConsumer> {

    /**
     * Create a {@link LongStream} over the remaining elements of this CloseablePrimitiveIteratorOfLong. Closing the
     * result will close this CloseablePrimitiveIteratorOfLong.
     *
     * @return A {@link LongStream} over the remaining contents of this iterator
     */
    default LongStream longStream() {
        return StreamSupport.longStream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * A re-usable, immutable CloseablePrimitiveIteratorOfLong with no elements.
     */
    CloseablePrimitiveIteratorOfLong EMPTY = new CloseablePrimitiveIteratorOfLong() {
        @Override
        public long nextLong() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a CloseablePrimitiveIteratorOfLong with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A CloseablePrimitiveIteratorOfLong with no elements
     */
    static CloseablePrimitiveIteratorOfLong empty() {
        return EMPTY;
    }

    /**
     * Create a CloseablePrimitiveIteratorOfLong over an array of {@code long}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A CloseablePrimitiveIteratorOfLong of {@code values}
     */
    static CloseablePrimitiveIteratorOfLong of(@NotNull final long... values) {
        Objects.requireNonNull(values);
        return new CloseablePrimitiveIteratorOfLong() {

            private int valueIndex;

            @Override
            public long nextLong() {
                return values[valueIndex++];
            }

            @Override
            public boolean hasNext() {
                return valueIndex < values.length;
            }
        };
    }

    /**
     * Create a CloseablePrimitiveIteratorOfLong that repeats {@code value}, {@code repeatCount} times. The result does
     * not need to be {@link #close() closed}.
     *
     * @param value The value to repeat
     * @param repeatCount The number of repetitions
     * @return A CloseablePrimitiveIteratorOfLong that repeats {@code value}, {@code repeatCount} times
     */
    static CloseablePrimitiveIteratorOfLong repeat(final long value, final long repeatCount) {
        return new CloseablePrimitiveIteratorOfLong() {

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
     * Create a CloseablePrimitiveIteratorOfLong that concatenates an array of non-{@code null} {@code subIterators}. The
     * result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
     *
     * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing an
     *        array, ensure that this iterator has full ownership.
     * @return A CloseablePrimitiveIteratorOfLong concatenating all elements from {@code subIterators}
     */
    static CloseablePrimitiveIteratorOfLong concat(@NotNull final CloseablePrimitiveIteratorOfLong... subIterators) {
        Objects.requireNonNull(subIterators);
        if (subIterators.length == 0) {
            return empty();
        }
        if (subIterators.length == 1) {
            return subIterators[0];
        }
        return new CloseablePrimitiveIteratorOfLong() {

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
     * Return a CloseablePrimitiveIteratorOfLong that concatenates the contents of any non-{@code null}
     * CloseablePrimitiveIteratorOfLong found amongst {@code first}, {@code second}, and {@code third}.
     *
     * @param first The first iterator to consider concatenating
     * @param second The second iterator to consider concatenating
     * @param third The third iterator to consider concatenating
     * @return A CloseablePrimitiveIteratorOfLong that concatenates all elements as specified
     */
    static CloseablePrimitiveIteratorOfLong maybeConcat(
            @Nullable final CloseablePrimitiveIteratorOfLong first,
            @Nullable final CloseablePrimitiveIteratorOfLong second,
            @Nullable final CloseablePrimitiveIteratorOfLong third) {
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
