//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.validation;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Interface for implementations of {@link ColumnExpressionValidator} to evaluate an individual method or constructor.
 */
public interface MethodInvocationValidator {
    /**
     * Evaluate whether the provided constructor should be permitted.
     * 
     * @param constructor the constructor to evaluate
     * @return true if the constructor should be permitted, false if it should not be permitted, or null if this
     *         validator has no opinion
     */
    Boolean permitConstructor(Constructor<?> constructor);

    /**
     * Evaluate whether the provided method should be permitted.
     * 
     * @param method the constructor to evaluate
     * @return true if the method should be permitted, false if it should not be permitted, or null if this validator
     *         has no opinion
     */
    Boolean permitMethod(Method method);
}
