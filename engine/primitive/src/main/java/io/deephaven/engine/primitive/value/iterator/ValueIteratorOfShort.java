//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ValueIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.value.iterator;

import io.deephaven.engine.primitive.function.ShortConsumer;
import io.deephaven.engine.primitive.function.ShortToIntFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfShort;
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

public interface ValueIteratorOfShort extends CloseablePrimitiveIteratorOfShort, ValueIterator<Short> {

    @Override
    @FinalDefault
    default Short next() {
        return TypeUtils.box(nextShort());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Short> action) {
        if (action instanceof ShortConsumer) {
            forEachRemaining((ShortConsumer) action);
        } else {
            forEachRemaining((final short element) -> action.accept(TypeUtils.box(element)));
        }
    }

    // region streamAsInt
    /**
     * Create an unboxed {@link IntStream} over the remaining elements of this ValueIteratorOfShort by casting each
     * element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_SHORT
     * NULL_SHORT} to {@link io.deephaven.util.QueryConstants#NULL_INT NULL_INT}. The result <em>must</em> be
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
                (final short value) -> value == QueryConstants.NULL_SHORT ? QueryConstants.NULL_INT : (int) value);
    }

    /**
     * Create an {@link IntStream} over the remaining elements of this ValueIteratorOfShort by applying {@code adapter}
     * to each element. The result <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to
     * ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return An {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    @FinalDefault
    default IntStream streamAsInt(@NotNull final ShortToIntFunction adapter) {
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
     * A re-usable, immutable ValueIteratorOfShort with no elements.
     */
    ValueIteratorOfShort EMPTY = new ValueIteratorOfShort() {
        @Override
        public short nextShort() {
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
     * Get a ValueIteratorOfShort with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A ValueIteratorOfShort with no elements
     */
    static ValueIteratorOfShort empty() {
        return EMPTY;
    }

    /**
     * Create a ValueIteratorOfShort over an array of {@code short}. The result does not need to be {@link #close()
     * closed}.
     *
     * @param values The elements to iterate
     * @return A ValueIteratorOfShort of {@code values}
     */
    static ValueIteratorOfShort of(@NotNull final short... values) {
        Objects.requireNonNull(values);
        return new ValueIteratorOfShort() {

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

            @Override
            public long remaining() {
                return values.length - valueIndex;
            }
        };
    }

    /**
     * Wraps a ValueIteratorOfShort with set number of prefix nulls, postfix nulls, or both. The result must be
     * {@link #close() closed}.
     *
     * @param iterator The ValueIteratorOfShort to wrap
     * @param prefixNulls The number of nulls to add to the beginning of the iterator
     * @param postfixNulls The number of nulls to add to the end of the iterator
     * @return A ValueIteratorOfShort with the specified number of prefix and postfix nulls
     */
    static ValueIteratorOfShort wrapWithNulls(
            @Nullable final ValueIteratorOfShort iterator,
            long prefixNulls,
            long postfixNulls) {

        if (prefixNulls == 0 && postfixNulls == 0) {
            return iterator == null ? ValueIteratorOfShort.empty() : iterator;
        }
        final long initialLength = prefixNulls + postfixNulls + (iterator == null ? 0 : iterator.remaining());
        return new ValueIteratorOfShort() {
            private long nextIndex = 0;

            @Override
            public short nextShort() {
                if (nextIndex >= initialLength) {
                    throw new NoSuchElementException();
                }
                if (nextIndex++ < prefixNulls || iterator == null || !iterator.hasNext()) {
                    return QueryConstants.NULL_SHORT;
                }
                return iterator.nextShort();
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
