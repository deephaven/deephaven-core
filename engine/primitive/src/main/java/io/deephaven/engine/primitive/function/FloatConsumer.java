package io.deephaven.engine.primitive.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.function.DoubleConsumer;

/**
 * Like {@link DoubleConsumer}, but for primitive floats.
 */
@FunctionalInterface
public interface FloatConsumer {

    /**
     * See {@link DoubleConsumer#accept(double)}.
     */
    void accept(float value);

    /**
     * See {@link DoubleConsumer#andThen(DoubleConsumer)}.
     */
    default FloatConsumer andThen(@NotNull final FloatConsumer after) {
        Objects.requireNonNull(after);
        return (float v) -> {
            accept(v);
            after.accept(v);
        };
    }
}
