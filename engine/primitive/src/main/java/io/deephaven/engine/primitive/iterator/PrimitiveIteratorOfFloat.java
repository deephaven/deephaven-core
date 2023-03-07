/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PrimitiveIteratorOfChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.FloatConsumer;
import io.deephaven.engine.primitive.function.FloatToDoubleFunction;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Like {@link PrimitiveIterator.OfDouble}, but for primitive floats.
 */
public interface PrimitiveIteratorOfFloat extends PrimitiveIterator<Float, FloatConsumer> {

    /**
     * See {@link PrimitiveIterator.OfDouble#nextDouble()}.
     */
    float nextFloat();

    /**
     * See {@link PrimitiveIterator.OfDouble#forEachRemaining(DoubleConsumer)}.
     */
    @Override
    default void forEachRemaining(@NotNull final FloatConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextFloat());
        }
    }

    /**
     * See {@link PrimitiveIterator.OfDouble#next()}.
     */
    @Override
    default Float next() {
        return nextFloat();
    }

    /**
     * See {@link PrimitiveIterator.OfDouble#forEachRemaining(Consumer)}.
     */
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
     * Adapt this PrimitiveIteratorOfFloat to a {@link PrimitiveIterator.OfDouble}, applying {@code adapter} to each
     * element.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default PrimitiveIterator.OfDouble adaptToOfDouble(@NotNull final FloatToDoubleFunction adapter) {
        return new OfDouble() {
            @Override
            public boolean hasNext() {
                return PrimitiveIteratorOfFloat.this.hasNext();
            }

            @Override
            public double nextDouble() {
                return adapter.applyAsDouble(PrimitiveIteratorOfFloat.this.next());
            }
        };
    }

    /**
     * Create a {@link DoubleStream} over the remaining elements of this PrimitiveIteratorOfFloat by applying
     * {@code adapter} to each element. Some implementations may require that the result be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator. May require {@link Stream#close()
     *         close}.
     */
    default DoubleStream streamAsDouble(@NotNull final FloatToDoubleFunction adapter) {
        final PrimitiveIterator.OfDouble adapted = adaptToOfDouble(adapter);
        return StreamSupport.doubleStream(
                Spliterators.spliteratorUnknownSize(
                        adapted,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false);
    }

    /**
     * Create a {@link DoubleStream} over the remaining elements of this PrimitiveIteratorOfFloat by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code double}
     * cast. Some implementations may require that the result be {@link java.util.stream.BaseStream#close() closed} in
     * order to ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator. May require {@link Stream#close()
     *         close}.
     */
    default DoubleStream streamAsDouble() {
        return streamAsDouble(value -> (int) value);
    }
}
