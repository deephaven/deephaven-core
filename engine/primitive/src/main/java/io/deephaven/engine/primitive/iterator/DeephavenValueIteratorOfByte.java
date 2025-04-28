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

public interface DeephavenValueIteratorOfByte extends CloseablePrimitiveIteratorOfByte {
    @Override
    @FinalDefault
    default Byte next() {
        return TypeUtils.box(nextByte());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Byte> action) {
        forEachRemaining((final byte element) -> action.accept(TypeUtils.box(element)));
    }

    // region streamAsInt
    /**
     * Create an unboxed {@link IntStream} over the remaining elements of this DeephavenValueIteratorOfByte by casting
     * each element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_BYTE
     * NULL_BYTE} to {@link io.deephaven.util.QueryConstants#NULL_INT NULL_INT}. The result <em>must</em> be
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
                (final byte value) -> value == QueryConstants.NULL_BYTE ? QueryConstants.NULL_INT : (int) value);
    }
    // endregion streamAsInt

    /**
     * A re-usable, immutable DeephavenValueIteratorOfByte with no elements.
     */
    DeephavenValueIteratorOfByte EMPTY = new DeephavenValueIteratorOfByte() {
        @Override
        public byte nextByte() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    };

    /**
     * Get a DeephavenValueIteratorOfByte with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A DeephavenValueIteratorOfByte with no elements
     */
    static DeephavenValueIteratorOfByte empty() {
        return EMPTY;
    }

    /**
     * Create a DeephavenValueIteratorOfByte over an array of {@code byte}. The result does not need to be
     * {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A DeephavenValueIteratorOfByte of {@code values}
     */
    static DeephavenValueIteratorOfByte of(@NotNull final byte... values) {
        Objects.requireNonNull(values);
        return new DeephavenValueIteratorOfByte() {

            private int valueIndex;

            @Override
            public byte nextByte() {
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
