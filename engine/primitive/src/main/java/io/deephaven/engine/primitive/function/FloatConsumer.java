/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharConsumer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.primitive.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Functional interface to apply an operation to a single {@code float}.
 */
@FunctionalInterface
public interface FloatConsumer {

    /**
     * Apply this operation to {@code value}.
     *
     * @param value The {@code float} to operate one
     */
    void accept(float value);

    /**
     * Return a composed FloatConsumer that applies {@code this} operation followed by {@code after}.
     *
     * @param after The FloatConsumer to apply after applying {@code this}
     * @return A composed FloatConsumer that applies {@code this} followed by {@code after}
     */
    default FloatConsumer andThen(@NotNull final FloatConsumer after) {
        Objects.requireNonNull(after);
        return (final float value) -> {
            accept(value);
            after.accept(value);
        };
    }
}
