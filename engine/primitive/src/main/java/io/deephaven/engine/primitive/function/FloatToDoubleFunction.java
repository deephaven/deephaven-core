//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatToIntFunction and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.function;

/**
 * Functional interface to apply a function to a single {@code float} input and produce a single {@code double} result.
 */
@FunctionalInterface
public interface FloatToDoubleFunction {

    /**
     * Apply this function to {@code value}.
     *
     * @param value The {@code float} input
     * @return The {@code double} result
     */
    double applyAsDouble(float value);
}
