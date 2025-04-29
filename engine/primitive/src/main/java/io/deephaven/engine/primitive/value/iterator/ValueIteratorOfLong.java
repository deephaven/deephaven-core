//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ValueIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.value.iterator;

import java.util.stream.LongStream;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
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

public interface ValueIteratorOfLong extends CloseablePrimitiveIteratorOfLong, ValueIterator<Long> {

    @Override
    @FinalDefault
    default Long next() {
        return TypeUtils.box(nextLong());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Long> action) {
        forEachRemaining((final long element) -> action.accept(TypeUtils.box(element)));
    }

    // region streamAsInt
    // endregion streamAsInt

    // region stream
        /**
     * Create an unboxed {@link LongStream} over the remaining elements of this ValueIteratorOfLong. The result
     * <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return An unboxed {@link LongStream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default LongStream longStream() {
        return StreamSupport.longStream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
    
    /**
     * Create a boxed {@link Stream} over the remaining elements of this ValueIteratorOfLong. The result <em>must</em>
     * be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default Stream<Long> stream() {
        return longStream().mapToObj(TypeUtils::box);
    }
    // endregion stream

    /**
     * A re-usable, immutable ValueIteratorOfLong with no elements.
     */
    ValueIteratorOfLong EMPTY = new ValueIteratorOfLong() {
        @Override
        public long nextLong() {
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
     * Get a ValueIteratorOfLong with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A ValueIteratorOfLong with no elements
     */
    static ValueIteratorOfLong empty() {
        return EMPTY;
    }

    /**
     * Create a ValueIteratorOfLong over an array of {@code long}. The result does not need to be {@link #close()
     * closed}.
     *
     * @param values The elements to iterate
     * @return A ValueIteratorOfLong of {@code values}
     */
    static ValueIteratorOfLong of(@NotNull final long... values) {
        Objects.requireNonNull(values);
        return new ValueIteratorOfLong() {

            private int valueIndex;

            @Override
            public long nextLong() {
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
