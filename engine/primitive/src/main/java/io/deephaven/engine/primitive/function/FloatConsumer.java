//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharConsumer and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
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
