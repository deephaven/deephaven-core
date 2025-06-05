//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.lang.reflect.Method;
import java.util.Set;


/**
 * A list of methods for the {@link MethodListInvocationValidator}.
 */
@Immutable
public abstract class MethodList {
    public static ImmutableMethodList.Builder builder() {
        return ImmutableMethodList.builder();
    }

    /**
     * All instance methods on the following target Classes are permitted.
     */
    @Parameter
    public abstract Set<Class<?>> instanceTargets();

    /**
     * All static methods on the following target Classes are permitted.
     */
    @Parameter
    public abstract Set<Class<?>> staticTargets();

    /**
     * A set of instance methods that are permitted.
     */
    @Parameter
    public abstract Set<Method> instanceMethods();

    /**
     * A set of static methods that are permitted.
     */
    @Parameter
    public abstract Set<Method> staticMethods();
}
