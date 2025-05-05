//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CloseablePrimitiveIteratorOfFloat and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.FloatConsumer;
import io.deephaven.engine.primitive.function.FloatToDoubleFunction;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code float}.
 */
public interface CloseablePrimitiveIteratorOfFloat extends CloseablePrimitiveIterator<Float, FloatConsumer> {

    /**
     * Returns the next {@code float} element in the iteration.
     *
     * @return The next {@code float} element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    float nextFloat();

    @Override
    default void forEachRemaining(@NotNull final FloatConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextFloat());
        }
    }

    @Override
    default Float next() {
        return nextFloat();
    }

    @Override
    default void forEachRemaining(@NotNull final Consumer<? super Float> action) {
        if (action instanceof FloatConsumer) {
            forEachRemaining((FloatConsumer) action);
        } else {
            Objects.requireNonNull(action);
            forEachRemaining((FloatConsumer) action::accept);
        }
    }

    /**
     * Adapt this CloseablePrimitiveIteratorOfFloat to a {@link CloseablePrimitiveIteratorOfDouble}, applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfFloat.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default CloseablePrimitiveIteratorOfDouble adaptToOfDouble(@NotNull final FloatToDoubleFunction adapter) {
        return new CloseablePrimitiveIteratorOfDouble() {

            @Override
            public boolean hasNext() {
                return CloseablePrimitiveIteratorOfFloat.this.hasNext();
            }

            @Override
            public double nextDouble() {
                return adapter.applyAsDouble(CloseablePrimitiveIteratorOfFloat.this.nextFloat());
            }

            @Override
            public void close() {
                CloseablePrimitiveIteratorOfFloat.this.close();
            }
        };
    }

    /**
     * Create a {@link DoubleStream} over the remaining elements of this CloseablePrimitiveIteratorOfFloat by applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfFloat.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator
     */
    default DoubleStream streamAsDouble(@NotNull final FloatToDoubleFunction adapter) {
        // noinspection resource
        return adaptToOfDouble(adapter).doubleStream();
    }

    /**
     * Create a {@link DoubleStream} over the remaining elements of this CloseablePrimitiveIteratorOfFloat by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code double}
     * cast. Closing the result will close this CloseablePrimitiveIteratorOfFloat.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator
     */
    default DoubleStream streamAsDouble() {
        return streamAsDouble(value -> (double) value);
    }

    /**
     * A re-usable, immutable CloseablePrimitiveIteratorOfFloat with no elements.
     */
    CloseablePrimitiveIteratorOfFloat EMPTY = new CloseablePrimitiveIteratorOfFloat() {
        @Override
        public float nextFloat() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a CloseablePrimitiveIteratorOfFloat with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A CloseablePrimitiveIteratorOfFloat with no elements
     */
    static CloseablePrimitiveIteratorOfFloat empty() {
        return EMPTY;
    }

    /**
     * Create a CloseablePrimitiveIteratorOfFloat over an array of {@code float}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A CloseablePrimitiveIteratorOfFloat of {@code values}
     */
    static CloseablePrimitiveIteratorOfFloat of(@NotNull final float... values) {
        Objects.requireNonNull(values);
        return new CloseablePrimitiveIteratorOfFloat() {

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
     * Create a CloseablePrimitiveIteratorOfFloat that repeats {@code value}, {@code repeatCount} times. The result does
     * not need to be {@link #close() closed}.
     *
     * @param value The value to repeat
     * @param repeatCount The number of repetitions
     * @return A CloseablePrimitiveIteratorOfFloat that repeats {@code value}, {@code repeatCount} times
     */
    static CloseablePrimitiveIteratorOfFloat repeat(final float value, final long repeatCount) {
        return new CloseablePrimitiveIteratorOfFloat() {

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
     * Create a CloseablePrimitiveIteratorOfFloat that concatenates an array of non-{@code null} {@code subIterators}.
     * The result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
     *
     * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing an
     *        array, ensure that this iterator has full ownership.
     * @return A CloseablePrimitiveIteratorOfFloat concatenating all elements from {@code subIterators}
     */
    static CloseablePrimitiveIteratorOfFloat concat(@NotNull final CloseablePrimitiveIteratorOfFloat... subIterators) {
        Objects.requireNonNull(subIterators);
        if (subIterators.length == 0) {
            return empty();
        }
        if (subIterators.length == 1) {
            return subIterators[0];
        }
        return new CloseablePrimitiveIteratorOfFloat() {

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
     * Return a CloseablePrimitiveIteratorOfFloat that concatenates the contents of any non-{@code null}
     * CloseablePrimitiveIteratorOfFloat found amongst {@code first}, {@code second}, and {@code third}.
     *
     * @param first The first iterator to consider concatenating
     * @param second The second iterator to consider concatenating
     * @param third The third iterator to consider concatenating
     * @return A CloseablePrimitiveIteratorOfFloat that concatenates all elements as specified
     */
    static CloseablePrimitiveIteratorOfFloat maybeConcat(
            @Nullable final CloseablePrimitiveIteratorOfFloat first,
            @Nullable final CloseablePrimitiveIteratorOfFloat second,
            @Nullable final CloseablePrimitiveIteratorOfFloat third) {
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
