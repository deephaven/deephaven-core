package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.primitive.function.CharToIntFunction;
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
 * Like {@link PrimitiveIterator.OfInt}, but for primitive chars.
 */
public interface PrimitiveIteratorOfChar extends PrimitiveIterator<Character, CharConsumer> {

    /**
     * See {@link PrimitiveIterator.OfInt#nextInt()}.
     */
    char nextChar();

    /**
     * See {@link PrimitiveIterator.OfInt#forEachRemaining(IntConsumer)}.
     */
    @Override
    default void forEachRemaining(@NotNull final CharConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextChar());
        }
    }

    /**
     * See {@link PrimitiveIterator.OfInt#next()}.
     */
    @Override
    default Character next() {
        return nextChar();
    }

    /**
     * See {@link PrimitiveIterator.OfInt#forEachRemaining(Consumer)}.
     */
    @Override
    default void forEachRemaining(@NotNull final Consumer<? super Character> action) {
        if (action instanceof CharConsumer) {
            forEachRemaining((CharConsumer) action);
        } else {
            Objects.requireNonNull(action);
            forEachRemaining((CharConsumer) action::accept);
        }
    }

    /**
     * Adapt this PrimitiveIteratorOfChar to a {@link PrimitiveIterator.OfInt}, applying {@code adapter} to each
     * element.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default PrimitiveIterator.OfInt adaptToOfInt(@NotNull final CharToIntFunction adapter) {
        return new OfInt() {
            @Override
            public boolean hasNext() {
                return PrimitiveIteratorOfChar.this.hasNext();
            }

            @Override
            public int nextInt() {
                return adapter.applyAsInt(PrimitiveIteratorOfChar.this.next());
            }
        };
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this PrimitiveIteratorOfChar by applying
     * {@code adapter} to each element. Some implementations may require that the result be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. May require {@link Stream#close()
     *         close}.
     */
    default IntStream streamAsInt(@NotNull final CharToIntFunction adapter) {
        final PrimitiveIterator.OfInt adapted = adaptToOfInt(adapter);
        return StreamSupport.intStream(
                Spliterators.spliteratorUnknownSize(
                        adapted,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false);
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this PrimitiveIteratorOfChar by applying an
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
