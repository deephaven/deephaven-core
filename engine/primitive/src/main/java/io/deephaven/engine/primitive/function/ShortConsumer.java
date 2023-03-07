package io.deephaven.engine.primitive.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.function.IntConsumer;

/**
 * Like {@link IntConsumer}, but for primitive shorts.
 */
@FunctionalInterface
public interface ShortConsumer {

    /**
     * See {@link IntConsumer#accept(int)}.
     */
    void accept(short value);

    /**
     * See {@link IntConsumer#andThen(IntConsumer)}.
     */
    default ShortConsumer andThen(@NotNull final ShortConsumer after) {
        Objects.requireNonNull(after);
        return (short v) -> {
            accept(v);
            after.accept(v);
        };
    }
}
