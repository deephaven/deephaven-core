//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharToIntFunction and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.function;

/**
 * Functional interface to apply a function to a single {@code short} input and produce a single {@code int} result.
 */
@FunctionalInterface
public interface ShortToIntFunction {

    /**
     * Apply this function to {@code value}.
     *
     * @param value The {@code short} input
     * @return The {@code int} result
     */
    int applyAsInt(short value);
}
