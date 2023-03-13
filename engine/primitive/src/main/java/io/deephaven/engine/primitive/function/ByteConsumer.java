/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharConsumer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.primitive.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Functional interface to apply an operation to a single {@code byte}.
 */
@FunctionalInterface
public interface ByteConsumer {

    /**
     * Apply this operation to {@code value}.
     *
     * @param value The {@code byte} to operate one
     */
    void accept(byte value);

    /**
     * Return a composed ByteConsumer that applies {@code this} operation followed by {@code after}.
     *
     * @param after The ByteConsumer to apply after applying {@code this}
     * @return A composed ByteConsumer that applies {@code this} followed by {@code after}
     */
    default ByteConsumer andThen(@NotNull final ByteConsumer after) {
        Objects.requireNonNull(after);
        return (final byte value) -> {
            accept(value);
            after.accept(value);
        };
    }
}
