//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ValueIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.value.iterator;

import java.util.function.IntConsumer;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
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

public interface ValueIteratorOfInt extends CloseablePrimitiveIteratorOfInt, ValueIterator<Integer> {

    @Override
    @FinalDefault
    default Integer next() {
        return TypeUtils.box(nextInt());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Integer> action) {
        if (action instanceof IntConsumer) {
            forEachRemaining((IntConsumer) action);
        } else {
            forEachRemaining((final int element) -> action.accept(TypeUtils.box(element)));
        }
    }

    // region streamAsInt
    /**
     * Create an unboxed {@link IntStream} over the remaining elements of this IntegerColumnIterator. The result
     * <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return An unboxed {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default IntStream intStream() {
        return StreamSupport.intStream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
    
    /**
     * Create a boxed {@link Stream} over the remaining elements of this DeephavenValueIteratorOfInt. 
     * The result <em>must</em>, be {@link java.util.stream.BaseStream#close() closed} in order 
     * to ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default Stream<Integer> stream() {
       return intStream().mapToObj(TypeUtils::box);
    }
    // endregion streamAsInt

    /**
     * A re-usable, immutable ValueIteratorOfInt with no elements.
     */
    ValueIteratorOfInt EMPTY = new ValueIteratorOfInt() {
        @Override
        public int nextInt() {
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
     * Get a ValueIteratorOfInt with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A ValueIteratorOfInt with no elements
     */
    static ValueIteratorOfInt empty() {
        return EMPTY;
    }

    /**
     * Create a ValueIteratorOfInt over an array of {@code int}. The result does not need to be {@link #close()
     * closed}.
     *
     * @param values The elements to iterate
     * @return A ValueIteratorOfInt of {@code values}
     */
    static ValueIteratorOfInt of(@NotNull final int... values) {
        Objects.requireNonNull(values);
        return new ValueIteratorOfInt() {

            private int valueIndex;

            @Override
            public int nextInt() {
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
     * Wraps a ValueIteratorOfInt with set number of prefix nulls, postfix nulls, or both. The result must be
     * {@link #close() closed}.
     *
     * @param iterator The ValueIteratorOfInt to wrap
     * @param prefixNulls The number of nulls to add to the beginning of the iterator
     * @param postfixNulls The number of nulls to add to the end of the iterator
     * @return A ValueIteratorOfInt with the specified number of prefix and postfix nulls
     */
    static ValueIteratorOfInt wrapWithNulls(
            @Nullable final ValueIteratorOfInt iterator,
            long prefixNulls,
            long postfixNulls) {

        if (prefixNulls == 0 && postfixNulls == 0) {
            return iterator == null ? ValueIteratorOfInt.empty() : iterator;
        }
        final long initialLength = prefixNulls + postfixNulls + (iterator == null ? 0 : iterator.remaining());
        return new ValueIteratorOfInt() {
            private long nextIndex = 0;

            @Override
            public int nextInt() {
                if (nextIndex >= initialLength) {
                    throw new NoSuchElementException();
                }
                if (nextIndex++ < prefixNulls || iterator == null || !iterator.hasNext()) {
                    return QueryConstants.NULL_INT;
                }
                return iterator.nextInt();
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
