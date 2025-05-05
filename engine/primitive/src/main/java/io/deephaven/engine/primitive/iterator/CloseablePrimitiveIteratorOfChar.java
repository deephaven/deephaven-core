//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.primitive.function.CharToIntFunction;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code char}.
 */
public interface CloseablePrimitiveIteratorOfChar extends CloseablePrimitiveIterator<Character, CharConsumer> {

    /**
     * Returns the next {@code char} element in the iteration.
     *
     * @return The next {@code char} element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    char nextChar();

    @Override
    default void forEachRemaining(@NotNull final CharConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextChar());
        }
    }

    @Override
    default Character next() {
        return nextChar();
    }

    @Override
    default void forEachRemaining(@NotNull final Consumer<? super Character> action) {
        if (action instanceof CharConsumer) {
            forEachRemaining((CharConsumer) action);
        } else {
            Objects.requireNonNull(action);
            forEachRemaining((CharConsumer) action::accept);
        }
    }

    /**
     * Adapt this CloseablePrimitiveIteratorOfChar to a {@link CloseablePrimitiveIteratorOfInt}, applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfChar.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default CloseablePrimitiveIteratorOfInt adaptToOfInt(@NotNull final CharToIntFunction adapter) {
        return new CloseablePrimitiveIteratorOfInt() {

            @Override
            public boolean hasNext() {
                return CloseablePrimitiveIteratorOfChar.this.hasNext();
            }

            @Override
            public int nextInt() {
                return adapter.applyAsInt(CloseablePrimitiveIteratorOfChar.this.nextChar());
            }

            @Override
            public void close() {
                CloseablePrimitiveIteratorOfChar.this.close();
            }
        };
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfChar by applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfChar.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt(@NotNull final CharToIntFunction adapter) {
        // noinspection resource
        return adaptToOfInt(adapter).intStream();
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfChar by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code int}
     * cast. Closing the result will close this CloseablePrimitiveIteratorOfChar.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt() {
        return streamAsInt(value -> (int) value);
    }

    /**
     * A re-usable, immutable CloseablePrimitiveIteratorOfChar with no elements.
     */
    CloseablePrimitiveIteratorOfChar EMPTY = new CloseablePrimitiveIteratorOfChar() {
        @Override
        public char nextChar() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a CloseablePrimitiveIteratorOfChar with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A CloseablePrimitiveIteratorOfChar with no elements
     */
    static CloseablePrimitiveIteratorOfChar empty() {
        return EMPTY;
    }

    /**
     * Create a CloseablePrimitiveIteratorOfChar over an array of {@code char}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A CloseablePrimitiveIteratorOfChar of {@code values}
     */
    static CloseablePrimitiveIteratorOfChar of(@NotNull final char... values) {
        Objects.requireNonNull(values);
        return new CloseablePrimitiveIteratorOfChar() {

            private int valueIndex;

            @Override
            public char nextChar() {
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
     * Create a CloseablePrimitiveIteratorOfChar that repeats {@code value}, {@code repeatCount} times. The result does
     * not need to be {@link #close() closed}.
     *
     * @param value The value to repeat
     * @param repeatCount The number of repetitions
     * @return A CloseablePrimitiveIteratorOfChar that repeats {@code value}, {@code repeatCount} times
     */
    static CloseablePrimitiveIteratorOfChar repeat(final char value, final long repeatCount) {
        return new CloseablePrimitiveIteratorOfChar() {

            private long repeatIndex;

            @Override
            public char nextChar() {
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
     * Create a CloseablePrimitiveIteratorOfChar that concatenates an array of non-{@code null} {@code subIterators}.
     * The result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
     *
     * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing an
     *        array, ensure that this iterator has full ownership.
     * @return A CloseablePrimitiveIteratorOfChar concatenating all elements from {@code subIterators}
     */
    static CloseablePrimitiveIteratorOfChar concat(@NotNull final CloseablePrimitiveIteratorOfChar... subIterators) {
        Objects.requireNonNull(subIterators);
        if (subIterators.length == 0) {
            return empty();
        }
        if (subIterators.length == 1) {
            return subIterators[0];
        }
        return new CloseablePrimitiveIteratorOfChar() {

            private boolean hasNextChecked;
            private int subIteratorIndex;

            @Override
            public char nextChar() {
                if (hasNext()) {
                    hasNextChecked = false;
                    return subIterators[subIteratorIndex].nextChar();
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
     * Return a CloseablePrimitiveIteratorOfChar that concatenates the contents of any non-{@code null}
     * CloseablePrimitiveIteratorOfChar found amongst {@code first}, {@code second}, and {@code third}.
     *
     * @param first The first iterator to consider concatenating
     * @param second The second iterator to consider concatenating
     * @param third The third iterator to consider concatenating
     * @return A CloseablePrimitiveIteratorOfChar that concatenates all elements as specified
     */
    static CloseablePrimitiveIteratorOfChar maybeConcat(
            @Nullable final CloseablePrimitiveIteratorOfChar first,
            @Nullable final CloseablePrimitiveIteratorOfChar second,
            @Nullable final CloseablePrimitiveIteratorOfChar third) {
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
