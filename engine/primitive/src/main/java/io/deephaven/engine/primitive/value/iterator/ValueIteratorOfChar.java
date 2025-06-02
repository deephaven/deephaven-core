//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.value.iterator;

import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.primitive.function.CharToIntFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfChar;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface ValueIteratorOfChar extends CloseablePrimitiveIteratorOfChar, ValueIterator<Character> {

    @Override
    @FinalDefault
    default Character next() {
        return TypeUtils.box(nextChar());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Character> action) {
        if (action instanceof CharConsumer) {
            forEachRemaining((CharConsumer) action);
        } else {
            forEachRemaining((final char element) -> action.accept(TypeUtils.box(element)));
        }
    }

    // region streamAsInt
    /**
     * Create an unboxed {@link IntStream} over the remaining elements of this ValueIteratorOfChar by casting each
     * element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_CHAR
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

    /**
     * Create an {@link IntStream} over the remaining elements of this ValueIteratorOfChar by applying {@code adapter}
     * to each element. The result <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to
     * ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return An {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    @FinalDefault
    default IntStream streamAsInt(@NotNull final CharToIntFunction adapter) {
        final PrimitiveIterator.OfInt adapted = adaptToOfInt(adapter);
        return StreamSupport.intStream(
                Spliterators.spliterator(
                        adapted,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
    // endregion streamAsInt

    /**
     * A re-usable, immutable ValueIteratorOfChar with no elements.
     */
    ValueIteratorOfChar EMPTY = new ValueIteratorOfChar() {
        @Override
        public char nextChar() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public long remaining() {
            return 0;
        }
    };

    /**
     * Get a ValueIteratorOfChar with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A ValueIteratorOfChar with no elements
     */
    static ValueIteratorOfChar empty() {
        return EMPTY;
    }

    /**
     * Create a ValueIteratorOfChar over an array of {@code char}. The result does not need to be {@link #close()
     * closed}.
     *
     * @param values The elements to iterate
     * @return A ValueIteratorOfChar of {@code values}
     */
    static ValueIteratorOfChar of(@NotNull final char... values) {
        Objects.requireNonNull(values);
        return new ValueIteratorOfChar() {

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

            @Override
            public long remaining() {
                return values.length - valueIndex;
            }
        };
    }

    /**
     * Wraps a ValueIteratorOfChar with set number of prefix nulls, postfix nulls, or both. The result must be
     * {@link #close() closed}.
     *
     * @param iterator The ValueIteratorOfChar to wrap
     * @param prefixNulls The number of nulls to add to the beginning of the iterator
     * @param postfixNulls The number of nulls to add to the end of the iterator
     * @return A ValueIteratorOfChar with the specified number of prefix and postfix nulls
     */
    static ValueIteratorOfChar wrapWithNulls(
            @Nullable final ValueIteratorOfChar iterator,
            long prefixNulls,
            long postfixNulls) {

        if (prefixNulls == 0 && postfixNulls == 0) {
            return iterator == null ? ValueIteratorOfChar.empty() : iterator;
        }
        final long initialLength = prefixNulls + postfixNulls + (iterator == null ? 0 : iterator.remaining());
        return new ValueIteratorOfChar() {
            private long nextIndex = 0;

            @Override
            public char nextChar() {
                if (nextIndex >= initialLength) {
                    throw new NoSuchElementException();
                }
                if (nextIndex++ < prefixNulls || iterator == null || !iterator.hasNext()) {
                    return QueryConstants.NULL_CHAR;
                }
                return iterator.nextChar();
            }

            @Override
            public boolean hasNext() {
                return nextIndex < initialLength;
            }

            @Override
            public long remaining() {
                return initialLength - nextIndex;
            }
        };
    }
}
