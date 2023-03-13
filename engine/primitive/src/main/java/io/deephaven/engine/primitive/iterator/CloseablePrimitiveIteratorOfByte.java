/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CloseablePrimitiveIteratorOfChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.ByteConsumer;
import io.deephaven.engine.primitive.function.ByteToIntFunction;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code byte}.
 */
public interface CloseablePrimitiveIteratorOfByte extends CloseablePrimitiveIterator<Byte, ByteConsumer> {

    /**
     * Returns the next {@code byte} element in the iteration.
     *
     * @return The next {@code byte} element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    byte nextByte();

    @Override
    default void forEachRemaining(@NotNull final ByteConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextByte());
        }
    }

    @Override
    default Byte next() {
        return nextByte();
    }

    @Override
    default void forEachRemaining(@NotNull final Consumer<? super Byte> action) {
        if (action instanceof ByteConsumer) {
            forEachRemaining((ByteConsumer) action);
        } else {
            Objects.requireNonNull(action);
            forEachRemaining((ByteConsumer) action::accept);
        }
    }

    /**
     * Adapt this CloseablePrimitiveIteratorOfByte to a {@link CloseablePrimitiveIteratorOfInt}, applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfByte.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default CloseablePrimitiveIteratorOfInt adaptToOfInt(@NotNull final ByteToIntFunction adapter) {
        return new CloseablePrimitiveIteratorOfInt() {

            @Override
            public boolean hasNext() {
                return CloseablePrimitiveIteratorOfByte.this.hasNext();
            }

            @Override
            public int nextInt() {
                return adapter.applyAsInt(CloseablePrimitiveIteratorOfByte.this.next());
            }

            @Override
            public void close() {
                CloseablePrimitiveIteratorOfByte.this.close();
            }
        };
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfByte by applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfByte.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt(@NotNull final ByteToIntFunction adapter) {
        // noinspection resource
        return adaptToOfInt(adapter).intStream();
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfByte by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code int}
     * cast. Closing the result will close this CloseablePrimitiveIteratorOfByte.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt() {
        return streamAsInt(value -> (int) value);
    }
}
