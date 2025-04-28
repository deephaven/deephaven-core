//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit DeephavenValueIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.iterator;

import java.util.stream.DoubleStream;

import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface DeephavenValueIteratorOfFloat extends CloseablePrimitiveIteratorOfFloat {
    @Override
    @FinalDefault
    default Float next() {
        return TypeUtils.box(nextFloat());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Float> action) {
        forEachRemaining((final float element) -> action.accept(TypeUtils.box(element)));
    }

    // region streamAsInt
    /**
     * Create an unboxed {@link DoubleStream} over the remaining elements of this DeephavenValueIteratorOfFloat by casting
     * each element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_FLOAT
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
    // endregion streamAsInt

    /**
     * A re-usable, immutable DeephavenValueIteratorOfFloat with no elements.
     */
    DeephavenValueIteratorOfFloat EMPTY = new DeephavenValueIteratorOfFloat() {
        @Override
        public float nextFloat() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a DeephavenValueIteratorOfFloat with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A DeephavenValueIteratorOfFloat with no elements
     */
    static DeephavenValueIteratorOfFloat empty() {
        return EMPTY;
    }

    /**
     * Create a DeephavenValueIteratorOfFloat over an array of {@code float}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A DeephavenValueIteratorOfFloat of {@code values}
     */
    static DeephavenValueIteratorOfFloat of(@NotNull final float... values) {
        Objects.requireNonNull(values);
        return new DeephavenValueIteratorOfFloat() {

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
        };
    }
}
