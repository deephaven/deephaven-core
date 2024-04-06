//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.function;

/**
 * Functional interface to apply a function to a single {@code char} input and produce a single {@code int} result.
 */
@FunctionalInterface
public interface CharToIntFunction {

    /**
     * Apply this function to {@code value}.
     *
     * @param value The {@code char} input
     * @return The {@code int} result
     */
    int applyAsInt(char value);
}
