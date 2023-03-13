/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CloseablePrimitiveIteratorOfChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.ShortConsumer;
import io.deephaven.engine.primitive.function.ShortToIntFunction;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code short}.
 */
public interface CloseablePrimitiveIteratorOfShort extends CloseablePrimitiveIterator<Short, ShortConsumer> {

    /**
     * Returns the next {@code short} element in the iteration.
     *
     * @return The next {@code short} element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    short nextShort();

    @Override
    default void forEachRemaining(@NotNull final ShortConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextShort());
        }
    }

    @Override
    default Short next() {
        return nextShort();
    }

    @Override
    default void forEachRemaining(@NotNull final Consumer<? super Short> action) {
        if (action instanceof ShortConsumer) {
            forEachRemaining((ShortConsumer) action);
        } else {
            Objects.requireNonNull(action);
            forEachRemaining((ShortConsumer) action::accept);
        }
    }

    /**
     * Adapt this CloseablePrimitiveIteratorOfShort to a {@link CloseablePrimitiveIteratorOfInt}, applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfShort.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default CloseablePrimitiveIteratorOfInt adaptToOfInt(@NotNull final ShortToIntFunction adapter) {
        return new CloseablePrimitiveIteratorOfInt() {

            @Override
            public boolean hasNext() {
                return CloseablePrimitiveIteratorOfShort.this.hasNext();
            }

            @Override
            public int nextInt() {
                return adapter.applyAsInt(CloseablePrimitiveIteratorOfShort.this.next());
            }

            @Override
            public void close() {
                CloseablePrimitiveIteratorOfShort.this.close();
            }
        };
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfShort by applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfShort.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt(@NotNull final ShortToIntFunction adapter) {
        // noinspection resource
        return adaptToOfInt(adapter).intStream();
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfShort by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code int}
     * cast. Closing the result will close this CloseablePrimitiveIteratorOfShort.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt() {
        return streamAsInt(value -> (int) value);
    }
}
