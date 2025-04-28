//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.iterator;

import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface DeephavenValueIteratorOfChar extends CloseablePrimitiveIteratorOfChar {
    @Override
    @FinalDefault
    default Character next() {
        return TypeUtils.box(nextChar());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Character> action) {
        forEachRemaining((final char element) -> action.accept(TypeUtils.box(element)));
    }

    // region streamAsInt
    /**
     * Create an unboxed {@link IntStream} over the remaining elements of this DeephavenValueIteratorOfChar by casting
     * each element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_CHAR
     * NULL_CHAR} to {@link io.deephaven.util.QueryConstants#NULL_INT NULL_INT}. The result <em>must</em> be
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
                (final char value) -> value == QueryConstants.NULL_CHAR ? QueryConstants.NULL_INT : (int) value);
    }
    // endregion streamAsInt

    /**
     * A re-usable, immutable DeephavenValueIteratorOfChar with no elements.
     */
    DeephavenValueIteratorOfChar EMPTY = new DeephavenValueIteratorOfChar() {
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
     * Get a DeephavenValueIteratorOfChar with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A DeephavenValueIteratorOfChar with no elements
     */
    static DeephavenValueIteratorOfChar empty() {
        return EMPTY;
    }

    /**
     * Create a DeephavenValueIteratorOfChar over an array of {@code char}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A DeephavenValueIteratorOfChar of {@code values}
     */
    static DeephavenValueIteratorOfChar of(@NotNull final char... values) {
        Objects.requireNonNull(values);
        return new DeephavenValueIteratorOfChar() {

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
}
