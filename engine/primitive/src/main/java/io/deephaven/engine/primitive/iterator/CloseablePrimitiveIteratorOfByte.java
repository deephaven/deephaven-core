//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CloseablePrimitiveIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.ByteConsumer;
import io.deephaven.engine.primitive.function.ByteToIntFunction;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code byte}.
 */
public interface CloseablePrimitiveIteratorOfByte extends CloseablePrimitiveIterator<Byte, ByteConsumer> {

    /**
     * Returns the next {@code byte} element in the iteration.
     *
     * @return The next {@code byte} element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    byte nextByte();

    @Override
    default void forEachRemaining(@NotNull final ByteConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextByte());
        }
    }

    @Override
    default Byte next() {
        return nextByte();
    }

    @Override
    default void forEachRemaining(@NotNull final Consumer<? super Byte> action) {
        if (action instanceof ByteConsumer) {
            forEachRemaining((ByteConsumer) action);
        } else {
            Objects.requireNonNull(action);
            forEachRemaining((ByteConsumer) action::accept);
        }
    }

    /**
     * Adapt this CloseablePrimitiveIteratorOfByte to a {@link CloseablePrimitiveIteratorOfInt}, applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfByte.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default CloseablePrimitiveIteratorOfInt adaptToOfInt(@NotNull final ByteToIntFunction adapter) {
        return new CloseablePrimitiveIteratorOfInt() {

            @Override
            public boolean hasNext() {
                return CloseablePrimitiveIteratorOfByte.this.hasNext();
            }

            @Override
            public int nextInt() {
                return adapter.applyAsInt(CloseablePrimitiveIteratorOfByte.this.nextByte());
            }

            @Override
            public void close() {
                CloseablePrimitiveIteratorOfByte.this.close();
            }
        };
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfByte by applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfByte.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt(@NotNull final ByteToIntFunction adapter) {
        // noinspection resource
        return adaptToOfInt(adapter).intStream();
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfByte by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code int}
     * cast. Closing the result will close this CloseablePrimitiveIteratorOfByte.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt() {
        return streamAsInt(value -> (int) value);
    }

    /**
     * A re-usable, immutable CloseablePrimitiveIteratorOfByte with no elements.
     */
    CloseablePrimitiveIteratorOfByte EMPTY = new CloseablePrimitiveIteratorOfByte() {
        @Override
        public byte nextByte() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a CloseablePrimitiveIteratorOfByte with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A CloseablePrimitiveIteratorOfByte with no elements
     */
    static CloseablePrimitiveIteratorOfByte empty() {
        return EMPTY;
    }

    /**
     * Create a CloseablePrimitiveIteratorOfByte over an array of {@code byte}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A CloseablePrimitiveIteratorOfByte of {@code values}
     */
    static CloseablePrimitiveIteratorOfByte of(@NotNull final byte... values) {
        Objects.requireNonNull(values);
        return new CloseablePrimitiveIteratorOfByte() {

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
     * Create a CloseablePrimitiveIteratorOfByte that repeats {@code value}, {@code repeatCount} times. The result does
     * not need to be {@link #close() closed}.
     *
     * @param value The value to repeat
     * @param repeatCount The number of repetitions
     * @return A CloseablePrimitiveIteratorOfByte that repeats {@code value}, {@code repeatCount} times
     */
    static CloseablePrimitiveIteratorOfByte repeat(final byte value, final long repeatCount) {
        return new CloseablePrimitiveIteratorOfByte() {

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
     * Create a CloseablePrimitiveIteratorOfByte that concatenates an array of non-{@code null} {@code subIterators}.
     * The result only needs to be {@link #close() closed} if any of the {@code subIterators} require it.
     *
     * @param subIterators The iterators to concatenate, none of which should be {@code null}. If directly passing an
     *        array, ensure that this iterator has full ownership.
     * @return A CloseablePrimitiveIteratorOfByte concatenating all elements from {@code subIterators}
     */
    static CloseablePrimitiveIteratorOfByte concat(@NotNull final CloseablePrimitiveIteratorOfByte... subIterators) {
        Objects.requireNonNull(subIterators);
        if (subIterators.length == 0) {
            return empty();
        }
        if (subIterators.length == 1) {
            return subIterators[0];
        }
        return new CloseablePrimitiveIteratorOfByte() {

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
     * Return a CloseablePrimitiveIteratorOfByte that concatenates the contents of any non-{@code null}
     * CloseablePrimitiveIteratorOfByte found amongst {@code first}, {@code second}, and {@code third}.
     *
     * @param first The first iterator to consider concatenating
     * @param second The second iterator to consider concatenating
     * @param third The third iterator to consider concatenating
     * @return A CloseablePrimitiveIteratorOfByte that concatenates all elements as specified
     */
    static CloseablePrimitiveIteratorOfByte maybeConcat(
            @Nullable final CloseablePrimitiveIteratorOfByte first,
            @Nullable final CloseablePrimitiveIteratorOfByte second,
            @Nullable final CloseablePrimitiveIteratorOfByte third) {
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
