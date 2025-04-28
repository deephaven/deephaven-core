//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit DeephavenValueIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
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

public interface DeephavenValueIteratorOfDouble extends CloseablePrimitiveIteratorOfDouble {
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

    /**
     * A re-usable, immutable DeephavenValueIteratorOfDouble with no elements.
     */
    DeephavenValueIteratorOfDouble EMPTY = new DeephavenValueIteratorOfDouble() {
        @Override
        public double nextDouble() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a DeephavenValueIteratorOfDouble with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A DeephavenValueIteratorOfDouble with no elements
     */
    static DeephavenValueIteratorOfDouble empty() {
        return EMPTY;
    }

    /**
     * Create a DeephavenValueIteratorOfDouble over an array of {@code double}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A DeephavenValueIteratorOfDouble of {@code values}
     */
    static DeephavenValueIteratorOfDouble of(@NotNull final double... values) {
        Objects.requireNonNull(values);
        return new DeephavenValueIteratorOfDouble() {

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
        };
    }
}
