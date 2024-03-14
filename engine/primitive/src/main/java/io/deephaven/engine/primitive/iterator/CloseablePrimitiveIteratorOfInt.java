//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.iterator;

import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code int}.
 */
public interface CloseablePrimitiveIteratorOfInt
        extends PrimitiveIterator.OfInt, CloseablePrimitiveIterator<Integer, IntConsumer> {

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfInt. Closing the
     * result will close this CloseablePrimitiveIteratorOfInt.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream intStream() {
        return StreamSupport.intStream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * A re-usable, immutable CloseablePrimitiveIteratorOfInt with no elements.
     */
    CloseablePrimitiveIteratorOfInt EMPTY = new CloseablePrimitiveIteratorOfInt() {
        @Override
        public int nextInt() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a CloseablePrimitiveIteratorOfInt with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A CloseablePrimitiveIteratorOfInt with no elements
     */
    static CloseablePrimitiveIteratorOfInt empty() {
        return EMPTY;
    }

    /**
     * Create a CloseablePrimitiveIteratorOfInt over an array of {@code int}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A CloseablePrimitiveIteratorOfInt of {@code values}
     */
    static CloseablePrimitiveIteratorOfInt of(@NotNull final int... values) {
        Objects.requireNonNull(values);
        return new CloseablePrimitiveIteratorOfInt() {

            private int valueIndex;

            @Override
            public int nextInt() {
                return values[valueIndex++];
            }

            @Override
            public boolean hasNext() {
                return valueIndex < values.length;
            }
        };
    }

    /**
     * Create a CloseablePrimitiveIteratorOfInt that repeats {@code value}, {@code repeatCount} times. The result does
     * not need to be {@link #close() closed}.
     *
     * @param value The value to repeat
     * @param repeatCount The number of repetitions
     * @return A CloseablePrimitiveIteratorOfInt that repeats {@code value}, {@code repeatCount} times
     */
    static CloseablePrimitiveIteratorOfInt repeat(final int value, final long repeatCount) {
        return new CloseablePrimitiveIteratorOfInt() {

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
     * Create a CloseablePrimitiveIteratorOfInt that concatenates an array of non-{@code null} {@code subIterators}. The
     * result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
     *
     * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing an
     *        array, ensure that this iterator has full ownership.
     * @return A CloseablePrimitiveIteratorOfInt concatenating all elements from {@code subIterators}
     */
    static CloseablePrimitiveIteratorOfInt concat(@NotNull final CloseablePrimitiveIteratorOfInt... subIterators) {
        Objects.requireNonNull(subIterators);
        if (subIterators.length == 0) {
            return empty();
        }
        if (subIterators.length == 1) {
            return subIterators[0];
        }
        return new CloseablePrimitiveIteratorOfInt() {

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
     * Return a CloseablePrimitiveIteratorOfInt that concatenates the contents of any non-{@code null}
     * CloseablePrimitiveIteratorOfInt found amongst {@code first}, {@code second}, and {@code third}.
     *
     * @param first The first iterator to consider concatenating
     * @param second The second iterator to consider concatenating
     * @param third The third iterator to consider concatenating
     * @return A CloseablePrimitiveIteratorOfInt that concatenates all elements as specified
     */
    static CloseablePrimitiveIteratorOfInt maybeConcat(
            @Nullable final CloseablePrimitiveIteratorOfInt first,
            @Nullable final CloseablePrimitiveIteratorOfInt second,
            @Nullable final CloseablePrimitiveIteratorOfInt third) {
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
