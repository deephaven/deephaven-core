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

public interface DeephavenValueIteratorOfLong extends CloseablePrimitiveIteratorOfLong {
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

    /**
     * A re-usable, immutable DeephavenValueIteratorOfLong with no elements.
     */
    DeephavenValueIteratorOfLong EMPTY = new DeephavenValueIteratorOfLong() {
        @Override
        public long nextLong() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a DeephavenValueIteratorOfLong with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A DeephavenValueIteratorOfLong with no elements
     */
    static DeephavenValueIteratorOfLong empty() {
        return EMPTY;
    }

    /**
     * Create a DeephavenValueIteratorOfLong over an array of {@code long}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A DeephavenValueIteratorOfLong of {@code values}
     */
    static DeephavenValueIteratorOfLong of(@NotNull final long... values) {
        Objects.requireNonNull(values);
        return new DeephavenValueIteratorOfLong() {

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
        };
    }
}
