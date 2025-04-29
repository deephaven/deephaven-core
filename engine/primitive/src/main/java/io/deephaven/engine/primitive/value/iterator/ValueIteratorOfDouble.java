//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ValueIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.value.iterator;

import java.util.stream.DoubleStream;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfDouble;
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

public interface ValueIteratorOfDouble extends CloseablePrimitiveIteratorOfDouble, ValueIterator<Double> {

    @Override
    @FinalDefault
    default Double next() {
        return TypeUtils.box(nextDouble());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Double> action) {
        forEachRemaining((final double element) -> action.accept(TypeUtils.box(element)));
    }

    // region streamAsInt
    // endregion streamAsInt

    // region stream
    /**
     * Create an unboxed {@link DoubleStream} over the remaining elements of this ValueIteratorOfDouble. The result
     * <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return An unboxed {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default DoubleStream doubleStream() {
        return StreamSupport.doubleStream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
    
    /**
     * Create a boxed {@link Stream} over the remaining elements of this ValueIteratorOfDouble. The result <em>must</em>
     * be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default Stream<Double> stream() {
        return doubleStream().mapToObj(TypeUtils::box);
    }
    // endregion stream

    /**
     * A re-usable, immutable ValueIteratorOfDouble with no elements.
     */
    ValueIteratorOfDouble EMPTY = new ValueIteratorOfDouble() {
        @Override
        public double nextDouble() {
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
     * Get a ValueIteratorOfDouble with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A ValueIteratorOfDouble with no elements
     */
    static ValueIteratorOfDouble empty() {
        return EMPTY;
    }

    /**
     * Create a ValueIteratorOfDouble over an array of {@code double}. The result does not need to be {@link #close()
     * closed}.
     *
     * @param values The elements to iterate
     * @return A ValueIteratorOfDouble of {@code values}
     */
    static ValueIteratorOfDouble of(@NotNull final double... values) {
        Objects.requireNonNull(values);
        return new ValueIteratorOfDouble() {

            private int valueIndex;

            @Override
            public double nextDouble() {
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
