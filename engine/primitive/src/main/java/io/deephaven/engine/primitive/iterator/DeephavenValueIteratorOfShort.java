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

public interface DeephavenValueIteratorOfShort extends CloseablePrimitiveIteratorOfShort {
    @Override
    @FinalDefault
    default Short next() {
        return TypeUtils.box(nextShort());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Short> action) {
        forEachRemaining((final short element) -> action.accept(TypeUtils.box(element)));
    }

    // region streamAsInt
    /**
     * Create an unboxed {@link IntStream} over the remaining elements of this DeephavenValueIteratorOfShort by casting
     * each element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_SHORT
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
    // endregion streamAsInt

    /**
     * A re-usable, immutable DeephavenValueIteratorOfShort with no elements.
     */
    DeephavenValueIteratorOfShort EMPTY = new DeephavenValueIteratorOfShort() {
        @Override
        public short nextShort() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a DeephavenValueIteratorOfShort with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A DeephavenValueIteratorOfShort with no elements
     */
    static DeephavenValueIteratorOfShort empty() {
        return EMPTY;
    }

    /**
     * Create a DeephavenValueIteratorOfShort over an array of {@code short}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A DeephavenValueIteratorOfShort of {@code values}
     */
    static DeephavenValueIteratorOfShort of(@NotNull final short... values) {
        Objects.requireNonNull(values);
        return new DeephavenValueIteratorOfShort() {

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
        };
    }
}
