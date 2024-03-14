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
 * Functional interface to apply an operation to a single {@code short}.
 */
@FunctionalInterface
public interface ShortConsumer {

    /**
     * Apply this operation to {@code value}.
     *
     * @param value The {@code short} to operate one
     */
    void accept(short value);

    /**
     * Return a composed ShortConsumer that applies {@code this} operation followed by {@code after}.
     *
     * @param after The ShortConsumer to apply after applying {@code this}
     * @return A composed ShortConsumer that applies {@code this} followed by {@code after}
     */
    default ShortConsumer andThen(@NotNull final ShortConsumer after) {
        Objects.requireNonNull(after);
        return (final short value) -> {
            accept(value);
            after.accept(value);
        };
    }
}
