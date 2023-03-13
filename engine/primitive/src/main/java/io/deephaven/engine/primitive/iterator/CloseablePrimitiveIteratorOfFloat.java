/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CloseablePrimitiveIteratorOfChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.FloatConsumer;
import io.deephaven.engine.primitive.function.FloatToDoubleFunction;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code float}.
 */
public interface CloseablePrimitiveIteratorOfFloat extends CloseablePrimitiveIterator<Float, FloatConsumer> {

    /**
     * Returns the next {@code float} element in the iteration.
     *
     * @return The next {@code float} element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    float nextFloat();

    @Override
    default void forEachRemaining(@NotNull final FloatConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextFloat());
        }
    }

    @Override
    default Float next() {
        return nextFloat();
    }

    @Override
    default void forEachRemaining(@NotNull final Consumer<? super Float> action) {
        if (action instanceof FloatConsumer) {
            forEachRemaining((FloatConsumer) action);
        } else {
            Objects.requireNonNull(action);
            forEachRemaining((FloatConsumer) action::accept);
        }
    }

    /**
     * Adapt this CloseablePrimitiveIteratorOfFloat to a {@link CloseablePrimitiveIteratorOfDouble}, applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfFloat.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default CloseablePrimitiveIteratorOfDouble adaptToOfDouble(@NotNull final FloatToDoubleFunction adapter) {
        return new CloseablePrimitiveIteratorOfDouble() {

            @Override
            public boolean hasNext() {
                return CloseablePrimitiveIteratorOfFloat.this.hasNext();
            }

            @Override
            public double nextDouble() {
                return adapter.applyAsDouble(CloseablePrimitiveIteratorOfFloat.this.next());
            }

            @Override
            public void close() {
                CloseablePrimitiveIteratorOfFloat.this.close();
            }
        };
    }

    /**
     * Create a {@link DoubleStream} over the remaining elements of this CloseablePrimitiveIteratorOfFloat by applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfFloat.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator
     */
    default DoubleStream streamAsDouble(@NotNull final FloatToDoubleFunction adapter) {
        // noinspection resource
        return adaptToOfDouble(adapter).doubleStream();
    }

    /**
     * Create a {@link DoubleStream} over the remaining elements of this CloseablePrimitiveIteratorOfFloat by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code double}
     * cast. Closing the result will close this CloseablePrimitiveIteratorOfFloat.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator
     */
    default DoubleStream streamAsDouble() {
        return streamAsDouble(value -> (double) value);
    }
}
