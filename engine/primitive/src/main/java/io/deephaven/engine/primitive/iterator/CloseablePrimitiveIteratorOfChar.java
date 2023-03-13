package io.deephaven.engine.primitive.iterator;

import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.primitive.function.CharToIntFunction;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code char}.
 */
public interface CloseablePrimitiveIteratorOfChar extends CloseablePrimitiveIterator<Character, CharConsumer> {

    /**
     * Returns the next {@code char} element in the iteration.
     *
     * @return The next {@code char} element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    char nextChar();

    @Override
    default void forEachRemaining(@NotNull final CharConsumer action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(nextChar());
        }
    }

    @Override
    default Character next() {
        return nextChar();
    }

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
     * Adapt this CloseablePrimitiveIteratorOfChar to a {@link CloseablePrimitiveIteratorOfInt}, applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfChar.
     *
     * @param adapter The adapter to apply
     * @return The adapted primitive iterator
     */
    default CloseablePrimitiveIteratorOfInt adaptToOfInt(@NotNull final CharToIntFunction adapter) {
        return new CloseablePrimitiveIteratorOfInt() {

            @Override
            public boolean hasNext() {
                return CloseablePrimitiveIteratorOfChar.this.hasNext();
            }

            @Override
            public int nextInt() {
                return adapter.applyAsInt(CloseablePrimitiveIteratorOfChar.this.next());
            }

            @Override
            public void close() {
                CloseablePrimitiveIteratorOfChar.this.close();
            }
        };
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfChar by applying
     * {@code adapter} to each element. Closing the result will close this CloseablePrimitiveIteratorOfChar.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt(@NotNull final CharToIntFunction adapter) {
        // noinspection resource
        return adaptToOfInt(adapter).intStream();
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfChar by applying an
     * implementation-defined default adapter to each element. The default implementation applies a simple {@code int}
     * cast. Closing the result will close this CloseablePrimitiveIteratorOfChar.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream streamAsInt() {
        return streamAsInt(value -> (int) value);
    }
}
