//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Functional interface to apply an operation to a single {@code char}.
 */
@FunctionalInterface
public interface CharConsumer {

    /**
     * Apply this operation to {@code value}.
     *
     * @param value The {@code char} to operate one
     */
    void accept(char value);

    /**
     * Return a composed CharConsumer that applies {@code this} operation followed by {@code after}.
     *
     * @param after The CharConsumer to apply after applying {@code this}
     * @return A composed CharConsumer that applies {@code this} followed by {@code after}
     */
    default CharConsumer andThen(@NotNull final CharConsumer after) {
        Objects.requireNonNull(after);
        return (final char value) -> {
            accept(value);
            after.accept(value);
        };
    }
}
