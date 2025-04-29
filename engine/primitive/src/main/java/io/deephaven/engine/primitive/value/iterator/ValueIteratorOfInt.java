//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ValueIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.value.iterator;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

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
        forEachRemaining((final int element) -> action.accept(TypeUtils.box(element)));
    }

    // region streamAsInt
    
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

    // region stream
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
    // endregion stream

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
}
