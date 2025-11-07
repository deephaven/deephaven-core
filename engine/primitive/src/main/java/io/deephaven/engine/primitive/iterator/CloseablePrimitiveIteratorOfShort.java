//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CloseablePrimitiveIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.ShortConsumer;
import io.deephaven.engine.primitive.function.ShortToIntFunction;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code short}.
 */
public interface CloseablePrimitiveIteratorOfShort extends CloseablePrimitiveIterator<Short, ShortConsumer> {

    /**
     * Returns the next {@code short} element in the iteration.
     *
     * @return The next {@code short} element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    short nextShort();

    @Override
    default void forEachRemaining(@NotNull final ShortConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextShort());
        }
    }

    @Override
    default Short next() {
        return nextShort();
    }

    @Override
    default void forEachRemaining(@NotNull final Consumer<? super Short> action) {
        if (action instanceof ShortConsumer) {
            forEachRemaining((ShortConsumer) action);
        } else {
            Objects.requireNonNull(action);
            forEachRemaining((ShortConsumer) action::accept);
        }
    }

    /**
     * Adapt this CloseablePrimitiveIteratorOfShort to a {@link CloseablePrimitiveIteratorOfInt}, applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfShort.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default CloseablePrimitiveIteratorOfInt adaptToOfInt(@NotNull final ShortToIntFunction adapter) {
        return new CloseablePrimitiveIteratorOfInt() {

            @Override
            public boolean hasNext() {
                return CloseablePrimitiveIteratorOfShort.this.hasNext();
            }

            @Override
            public int nextInt() {
                return adapter.applyAsInt(CloseablePrimitiveIteratorOfShort.this.nextShort());
            }

            @Override
            public void close() {
                CloseablePrimitiveIteratorOfShort.this.close();
            }
        };
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfShort by applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfShort.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt(@NotNull final ShortToIntFunction adapter) {
        // noinspection resource
        return adaptToOfInt(adapter).intStream();
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfShort by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code int}
     * cast. Closing the result will close this CloseablePrimitiveIteratorOfShort.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt() {
        return streamAsInt(value -> (int) value);
    }

    /**
     * A re-usable, immutable CloseablePrimitiveIteratorOfShort with no elements.
     */
    CloseablePrimitiveIteratorOfShort EMPTY = new CloseablePrimitiveIteratorOfShort() {
        @Override
        public short nextShort() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a CloseablePrimitiveIteratorOfShort with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A CloseablePrimitiveIteratorOfShort with no elements
     */
    static CloseablePrimitiveIteratorOfShort empty() {
        return EMPTY;
    }

    /**
     * Create a CloseablePrimitiveIteratorOfShort over an array of {@code short}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A CloseablePrimitiveIteratorOfShort of {@code values}
     */
    static CloseablePrimitiveIteratorOfShort of(@NotNull final short... values) {
        Objects.requireNonNull(values);
        return new CloseablePrimitiveIteratorOfShort() {

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
     * Create a CloseablePrimitiveIteratorOfShort that repeats {@code value}, {@code repeatCount} times. The result does
     * not need to be {@link #close() closed}.
     *
     * @param value The value to repeat
     * @param repeatCount The number of repetitions
     * @return A CloseablePrimitiveIteratorOfShort that repeats {@code value}, {@code repeatCount} times
     */
    static CloseablePrimitiveIteratorOfShort repeat(final short value, final long repeatCount) {
        return new CloseablePrimitiveIteratorOfShort() {

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
     * Create a CloseablePrimitiveIteratorOfShort that concatenates an array of non-{@code null} {@code subIterators}.
     * The result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
     *
     * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing an
     *        array, ensure that this iterator has full ownership.
     * @return A CloseablePrimitiveIteratorOfShort concatenating all elements from {@code subIterators}
     */
    static CloseablePrimitiveIteratorOfShort concat(@NotNull final CloseablePrimitiveIteratorOfShort... subIterators) {
        Objects.requireNonNull(subIterators);
        if (subIterators.length == 0) {
            return empty();
        }
        if (subIterators.length == 1) {
            return subIterators[0];
        }
        return new CloseablePrimitiveIteratorOfShort() {

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
     * Return a CloseablePrimitiveIteratorOfShort that concatenates the contents of any non-{@code null}
     * CloseablePrimitiveIteratorOfShort found amongst {@code first}, {@code second}, and {@code third}.
     *
     * @param first The first iterator to consider concatenating
     * @param second The second iterator to consider concatenating
     * @param third The third iterator to consider concatenating
     * @return A CloseablePrimitiveIteratorOfShort that concatenates all elements as specified
     */
    static CloseablePrimitiveIteratorOfShort maybeConcat(
            @Nullable final CloseablePrimitiveIteratorOfShort first,
            @Nullable final CloseablePrimitiveIteratorOfShort second,
            @Nullable final CloseablePrimitiveIteratorOfShort third) {
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
