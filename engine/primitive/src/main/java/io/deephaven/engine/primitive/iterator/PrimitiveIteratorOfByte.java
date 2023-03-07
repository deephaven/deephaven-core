/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PrimitiveIteratorOfChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.ByteConsumer;
import io.deephaven.engine.primitive.function.ByteToIntFunction;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Like {@link PrimitiveIterator.OfInt}, but for primitive bytes.
 */
public interface PrimitiveIteratorOfByte extends PrimitiveIterator<Byte, ByteConsumer> {

    /**
     * See {@link PrimitiveIterator.OfInt#nextInt()}.
     */
    byte nextByte();

    /**
     * See {@link PrimitiveIterator.OfInt#forEachRemaining(IntConsumer)}.
     */
    @Override
    default void forEachRemaining(@NotNull final ByteConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextByte());
        }
    }

    /**
     * See {@link PrimitiveIterator.OfInt#next()}.
     */
    @Override
    default Byte next() {
        return nextByte();
    }

    /**
     * See {@link PrimitiveIterator.OfInt#forEachRemaining(Consumer)}.
     */
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
     * Adapt this PrimitiveIteratorOfByte to a {@link PrimitiveIterator.OfInt}, applying {@code adapter} to each
     * element.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default PrimitiveIterator.OfInt adaptToOfInt(@NotNull final ByteToIntFunction adapter) {
        return new OfInt() {
            @Override
            public boolean hasNext() {
                return PrimitiveIteratorOfByte.this.hasNext();
            }

            @Override
            public int nextInt() {
                return adapter.applyAsInt(PrimitiveIteratorOfByte.this.next());
            }
        };
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this PrimitiveIteratorOfByte by applying
     * {@code adapter} to each element. Some implementations may require that the result be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. May require {@link Stream#close()
     *         close}.
     */
    default IntStream streamAsInt(@NotNull final ByteToIntFunction adapter) {
        final PrimitiveIterator.OfInt adapted = adaptToOfInt(adapter);
        return StreamSupport.intStream(
                Spliterators.spliteratorUnknownSize(
                        adapted,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false);
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this PrimitiveIteratorOfByte by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code int}
     * cast. Some implementations may require that the result be {@link java.util.stream.BaseStream#close() closed} in
     * order to ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. May require {@link Stream#close()
     *         close}.
     */
    default IntStream streamAsInt() {
        return streamAsInt(value -> (int) value);
    }
}
