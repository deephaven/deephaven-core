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
import java.util.function.DoubleConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code double}.
 */
public interface CloseablePrimitiveIteratorOfDouble
        extends PrimitiveIterator.OfDouble, CloseablePrimitiveIterator<Double, DoubleConsumer> {

    /**
     * Create a {@link DoubleStream} over the remaining elements of this CloseablePrimitiveIteratorOfDouble. Closing the
     * result will close this CloseablePrimitiveIteratorOfDouble.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator
     */
    default DoubleStream doubleStream() {
        return StreamSupport.doubleStream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * A re-usable, immutable CloseablePrimitiveIteratorOfDouble with no elements.
     */
    CloseablePrimitiveIteratorOfDouble EMPTY = new CloseablePrimitiveIteratorOfDouble() {
        @Override
        public double nextDouble() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a CloseablePrimitiveIteratorOfDouble with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A CloseablePrimitiveIteratorOfDouble with no elements
     */
    static CloseablePrimitiveIteratorOfDouble empty() {
        return EMPTY;
    }

    /**
     * Create a CloseablePrimitiveIteratorOfDouble over an array of {@code double}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A CloseablePrimitiveIteratorOfDouble of {@code values}
     */
    static CloseablePrimitiveIteratorOfDouble of(@NotNull final double... values) {
        Objects.requireNonNull(values);
        return new CloseablePrimitiveIteratorOfDouble() {

            private int valueIndex;

            @Override
            public double nextDouble() {
                return values[valueIndex++];
            }

            @Override
            public boolean hasNext() {
                return valueIndex < values.length;
            }
        };
    }

    /**
     * Create a CloseablePrimitiveIteratorOfDouble that repeats {@code value}, {@code repeatCount} times. The result does
     * not need to be {@link #close() closed}.
     *
     * @param value The value to repeat
     * @param repeatCount The number of repetitions
     * @return A CloseablePrimitiveIteratorOfDouble that repeats {@code value}, {@code repeatCount} times
     */
    static CloseablePrimitiveIteratorOfDouble repeat(final double value, final long repeatCount) {
        return new CloseablePrimitiveIteratorOfDouble() {

            private long repeatIndex;

            @Override
            public double nextDouble() {
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
     * Create a CloseablePrimitiveIteratorOfDouble that concatenates an array of non-{@code null} {@code subIterators}. The
     * result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
     *
     * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing an
     *        array, ensure that this iterator has full ownership.
     * @return A CloseablePrimitiveIteratorOfDouble concatenating all elements from {@code subIterators}
     */
    static CloseablePrimitiveIteratorOfDouble concat(@NotNull final CloseablePrimitiveIteratorOfDouble... subIterators) {
        Objects.requireNonNull(subIterators);
        if (subIterators.length == 0) {
            return empty();
        }
        if (subIterators.length == 1) {
            return subIterators[0];
        }
        return new CloseablePrimitiveIteratorOfDouble() {

            private boolean hasNextChecked;
            private int subIteratorIndex;

            @Override
            public double nextDouble() {
                if (hasNext()) {
                    hasNextChecked = false;
                    return subIterators[subIteratorIndex].nextDouble();
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
     * Return a CloseablePrimitiveIteratorOfDouble that concatenates the contents of any non-{@code null}
     * CloseablePrimitiveIteratorOfDouble found amongst {@code first}, {@code second}, and {@code third}.
     *
     * @param first The first iterator to consider concatenating
     * @param second The second iterator to consider concatenating
     * @param third The third iterator to consider concatenating
     * @return A CloseablePrimitiveIteratorOfDouble that concatenates all elements as specified
     */
    static CloseablePrimitiveIteratorOfDouble maybeConcat(
            @Nullable final CloseablePrimitiveIteratorOfDouble first,
            @Nullable final CloseablePrimitiveIteratorOfDouble second,
            @Nullable final CloseablePrimitiveIteratorOfDouble third) {
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
