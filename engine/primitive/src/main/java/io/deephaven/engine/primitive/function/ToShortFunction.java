//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ToCharFunction and run "./gradlew replicatePrimitiveInterfaces" to regenerate
//
// @formatter:off
package io.deephaven.engine.primitive.function;

/**
 * Functional interface to apply an operation to an object and produce a {@code short}.
 *
 * @param <T> the object type that this function applies to
 */
@FunctionalInterface
public interface ToShortFunction<T> {
    /**
     * Applies this function to the given argument of type {@link T}.
     * 
     * @param value the argument to the function
     * @return the short result
     */
    short applyAsShort(T value);
}
