//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ValueIteratorOfChar and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.value.iterator;

import io.deephaven.engine.primitive.function.ByteToIntFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte;
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

public interface ValueIteratorOfByte extends CloseablePrimitiveIteratorOfByte, ValueIterator<Byte> {

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
     * Create an unboxed {@link IntStream} over the remaining elements of this ValueIteratorOfByte by casting each
     * element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_BYTE
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

    /**
     * Create a {@link IntStream} over the remaining elements of this ByteColumnIterator by applying
     * {@code adapter} to each element. The result <em>must</em> be {@link java.util.stream.BaseStream#close() closed}
     * in order to ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    @FinalDefault
    default IntStream streamAsInt(@NotNull final ByteToIntFunction adapter) {
        final PrimitiveIterator.OfInt adapted = adaptToOfInt(adapter);
        return StreamSupport.intStream(
                Spliterators.spliterator(
                        adapted,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
    // endregion streamAsInt

    // region stream
    /**
     * Create a boxed {@link Stream} over the remaining elements of this ValueIteratorOfByte. The result <em>must</em>
     * be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default Stream<Byte> stream() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
    // endregion stream

    /**
     * A re-usable, immutable ValueIteratorOfByte with no elements.
     */
    ValueIteratorOfByte EMPTY = new ValueIteratorOfByte() {
        @Override
        public byte nextByte() {
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
     * Get a ValueIteratorOfByte with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A ValueIteratorOfByte with no elements
     */
    static ValueIteratorOfByte empty() {
        return EMPTY;
    }

    /**
     * Create a ValueIteratorOfByte over an array of {@code byte}. The result does not need to be {@link #close()
     * closed}.
     *
     * @param values The elements to iterate
     * @return A ValueIteratorOfByte of {@code values}
     */
    static ValueIteratorOfByte of(@NotNull final byte... values) {
        Objects.requireNonNull(values);
        return new ValueIteratorOfByte() {

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

            @Override
            public long remaining() {
                return values.length - valueIndex;
            }
        };
    }
}
