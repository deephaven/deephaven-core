package io.deephaven.engine.primitive.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.function.IntConsumer;

/**
 * Like {@link IntConsumer}, but for primitive chars.
 */
@FunctionalInterface
public interface CharConsumer {

    /**
     * See {@link IntConsumer#accept(int)}.
     */
    void accept(char value);

    /**
     * See {@link IntConsumer#andThen(IntConsumer)}.
     */
    default CharConsumer andThen(@NotNull final CharConsumer after) {
        Objects.requireNonNull(after);
        return (char v) -> {
            accept(v);
            after.accept(v);
        };
    }
}
