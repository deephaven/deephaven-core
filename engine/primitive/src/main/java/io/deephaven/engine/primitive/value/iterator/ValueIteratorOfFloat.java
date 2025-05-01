//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ValueIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.value.iterator;

import java.util.stream.DoubleStream;
import io.deephaven.engine.primitive.function.FloatToDoubleFunction;

import io.deephaven.engine.primitive.function.FloatConsumer;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfFloat;
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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface ValueIteratorOfFloat extends CloseablePrimitiveIteratorOfFloat, ValueIterator<Float> {

    @Override
    @FinalDefault
    default Float next() {
        return TypeUtils.box(nextFloat());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Float> action) {
        if (action instanceof FloatConsumer) {
            forEachRemaining((FloatConsumer) action);
        } else {
            forEachRemaining((final float element) -> action.accept(TypeUtils.box(element)));
        }
    }

    // region streamAsInt
    /**
     * Create an unboxed {@link DoubleStream} over the remaining elements of this ValueIteratorOfFloat by casting each
     * element to {@code double} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_FLOAT
     * NULL_FLOAT} to {@link io.deephaven.util.QueryConstants#NULL_DOUBLE NULL_DOUBLE}. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return An unboxed {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default DoubleStream streamAsDouble() {
        return streamAsDouble(
                (final float value) -> value == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_DOUBLE : (double) value);
    }

    /**
     * Create an {@link DoubleStream} over the remaining elements of this ValueIteratorOfFloat by applying {@code adapter}
     * to each element. The result <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to
     * ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return An {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    @FinalDefault
    default DoubleStream streamAsDouble(@NotNull final FloatToDoubleFunction adapter) {
        final PrimitiveIterator.OfDouble adapted = adaptToOfDouble(adapter);
        return StreamSupport.doubleStream(
                Spliterators.spliterator(
                        adapted,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
    // endregion streamAsInt

    /**
     * A re-usable, immutable ValueIteratorOfFloat with no elements.
     */
    ValueIteratorOfFloat EMPTY = new ValueIteratorOfFloat() {
        @Override
        public float nextFloat() {
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
     * Get a ValueIteratorOfFloat with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A ValueIteratorOfFloat with no elements
     */
    static ValueIteratorOfFloat empty() {
        return EMPTY;
    }

    /**
     * Create a ValueIteratorOfFloat over an array of {@code float}. The result does not need to be {@link #close()
     * closed}.
     *
     * @param values The elements to iterate
     * @return A ValueIteratorOfFloat of {@code values}
     */
    static ValueIteratorOfFloat of(@NotNull final float... values) {
        Objects.requireNonNull(values);
        return new ValueIteratorOfFloat() {

            private int valueIndex;

            @Override
            public float nextFloat() {
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
     * Wraps a ValueIteratorOfFloat with set number of prefix nulls, postfix nulls, or both. The result must be
     * {@link #close() closed}.
     *
     * @param iterator The ValueIteratorOfFloat to wrap
     * @param prefixNulls The number of nulls to add to the beginning of the iterator
     * @param postfixNulls The number of nulls to add to the end of the iterator
     * @return A ValueIteratorOfFloat with the specified number of prefix and postfix nulls
     */
    static ValueIteratorOfFloat wrapWithNulls(
            @Nullable final ValueIteratorOfFloat iterator,
            long prefixNulls,
            long postfixNulls) {

        if (prefixNulls == 0 && postfixNulls == 0) {
            return iterator == null ? ValueIteratorOfFloat.empty() : iterator;
        }
        final long initialLength = prefixNulls + postfixNulls + (iterator == null ? 0 : iterator.remaining());
        return new ValueIteratorOfFloat() {
            private long nextIndex = 0;

            @Override
            public float nextFloat() {
                if (nextIndex >= initialLength) {
                    throw new NoSuchElementException();
                }
                if (nextIndex++ < prefixNulls || iterator == null || !iterator.hasNext()) {
                    return QueryConstants.NULL_FLOAT;
                }
                return iterator.nextFloat();
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
