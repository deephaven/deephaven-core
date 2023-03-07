package io.deephaven.engine.primitive.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.function.IntConsumer;

/**
 * Like {@link IntConsumer}, but for primitive bytes.
 */
@FunctionalInterface
public interface ByteConsumer {

    /**
     * See {@link IntConsumer#accept(int)}.
     */
    void accept(byte value);

    /**
     * See {@link IntConsumer#andThen(IntConsumer)}.
     */
    default ByteConsumer andThen(@NotNull final ByteConsumer after) {
        Objects.requireNonNull(after);
        return (byte v) -> {
            accept(v);
            after.accept(v);
        };
    }
}
