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

public interface DeephavenValueIteratorOfInt extends CloseablePrimitiveIteratorOfInt {
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
        * Create a boxed {@link Stream} over the remaining elements of this IntegerColumnIterator. The result <em>must</em>
        * be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
        * try-with-resources block is strongly encouraged.
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
     * A re-usable, immutable DeephavenValueIteratorOfInt with no elements.
     */
    DeephavenValueIteratorOfInt EMPTY = new DeephavenValueIteratorOfInt() {
        @Override
        public int nextInt() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a DeephavenValueIteratorOfInt with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A DeephavenValueIteratorOfInt with no elements
     */
    static DeephavenValueIteratorOfInt empty() {
        return EMPTY;
    }

    /**
     * Create a DeephavenValueIteratorOfInt over an array of {@code int}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A DeephavenValueIteratorOfInt of {@code values}
     */
    static DeephavenValueIteratorOfInt of(@NotNull final int... values) {
        Objects.requireNonNull(values);
        return new DeephavenValueIteratorOfInt() {

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
        };
    }
}
