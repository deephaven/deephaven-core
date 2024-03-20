//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.iterator;

import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This interface extends {@link Iterator} and {@link AutoCloseable} in order to allow for iterators that acquire
 * resources that must be released. Such iterators must document this need, and appropriate measures (e.g. a
 * try-with-resources block) should be taken to ensure that they are {@link #close() closed}. Methods that return
 * streams over CloseableIterator instances should ensure that closing the resulting {@link Stream},
 * {@link java.util.stream.IntStream IntStream}, {@link java.util.stream.LongStream LongStream}, or
 * {@link java.util.stream.DoubleStream DoubleStream} will also close the iterator.
 */
public interface CloseableIterator<TYPE> extends Iterator<TYPE>, AutoCloseable {

    /**
     * Create a {@link Stream} over the remaining elements of this CloseableIterator. Closing the result will close this
     * CloseableIterator.
     *
     * @return A {@link Stream} over the remaining contents of this iterator
     */
    default Stream<TYPE> stream() {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    @Override
    default void close() {}

    /**
     * A re-usable, immutable CloseableIterator with no elements.
     */
    CloseableIterator<?> EMPTY = new CloseableIterator<>() {
        @Override
        public Object next() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a CloseableIterator with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A CloseableIterator with no elements
     */
    static <TYPE> CloseableIterator<TYPE> empty() {
        // noinspection unchecked
        return (CloseableIterator<TYPE>) EMPTY;
    }

    /**
     * Create a CloseableIterator over an array of {@code int}. The result does not need to be {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A CloseableIterator of {@code values}
     */
    @SafeVarargs
    static <TYPE> CloseableIterator<TYPE> of(@NotNull final TYPE... values) {
        Objects.requireNonNull(values);
        return new CloseableIterator<>() {

            private int valueIndex;

            @Override
            public TYPE next() {
                return values[valueIndex++];
            }

            @Override
            public boolean hasNext() {
                return valueIndex < values.length;
            }
        };
    }

    /**
     * Create a CloseableIterator that repeats {@code value}, {@code repeatCount} times. The result does not need to be
     * {@link #close() closed}.
     *
     * @param value The value to repeat
     * @param repeatCount The number of repetitions
     * @return A CloseableIterator that repeats {@code value}, {@code repeatCount} times
     */
    static <TYPE> CloseableIterator<TYPE> repeat(final TYPE value, final long repeatCount) {
        return new CloseableIterator<>() {

            private long repeatIndex;

            @Override
            public TYPE next() {
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
     * Create a CloseableIterator that concatenates an array of non-{@code null} {@code subIterators}. The result only
     * needs to be {@link #close() closed} if any of the {@code subIterators} require it.
     *
     * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing an
     *        array, ensure that this iterator has full ownership.
     * @return A CloseableIterator concatenating all elements from {@code subIterators}
     */
    @SafeVarargs
    static <TYPE> CloseableIterator<TYPE> concat(@NotNull final CloseableIterator<TYPE>... subIterators) {
        Objects.requireNonNull(subIterators);
        if (subIterators.length == 0) {
            return empty();
        }
        if (subIterators.length == 1) {
            return subIterators[0];
        }
        return new CloseableIterator<>() {

            private boolean hasNextChecked;
            private int subIteratorIndex;

            @Override
            public TYPE next() {
                if (hasNext()) {
                    hasNextChecked = false;
                    return subIterators[subIteratorIndex].next();
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
     * Return a CloseableIterator that concatenates the contents of any non-{@code null} CloseableIterator found amongst
     * {@code first}, {@code second}, and {@code third}.
     *
     * @param first The first iterator to consider concatenating
     * @param second The second iterator to consider concatenating
     * @param third The third iterator to consider concatenating
     * @return A CloseableIterator that concatenates all elements as specified
     */
    static <TYPE> CloseableIterator<TYPE> maybeConcat(
            @Nullable final CloseableIterator<TYPE> first,
            @Nullable final CloseableIterator<TYPE> second,
            @Nullable final CloseableIterator<TYPE> third) {
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
