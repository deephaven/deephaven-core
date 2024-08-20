//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Variable scope used to resolve parameter values during query execution and to expose named objects to users. Objects
 * passed in will have their liveness managed by the scope.
 */
public interface QueryScope extends LivenessNode, LogOutputAppendable {

    /**
     * Adds a parameter to the default instance {@link QueryScope}, or updates the value of an existing parameter.
     * Objects passed in will have their liveness managed by the scope.
     *
     * @param name String name of the parameter to add.
     * @param value value to assign to the parameter.
     * @param <T> type of the parameter/value.
     */
    static <T> void addParam(final String name, final T value) {
        ExecutionContext.getContext().getQueryScope().putParam(name, value);
    }

    /**
     * Gets a parameter from the default instance {@link QueryScope}.
     *
     * @param name parameter name.
     * @param <T> parameter type.
     * @return parameter value.
     * @throws MissingVariableException variable name is not defined.
     */
    static <T> T getParamValue(final String name) throws MissingVariableException {
        return ExecutionContext.getContext().getQueryScope().readParamValue(name);
    }

    /**
     * A type of RuntimeException thrown when a variable referenced within the {@link QueryScope} is not defined or,
     * more likely, has not been added to the scope.
     */
    class MissingVariableException extends RuntimeException {

        public MissingVariableException(final String message, final Throwable cause) {
            super(message, cause);
        }

        public MissingVariableException(final String message) {
            super(message);
        }

        private MissingVariableException(final Throwable cause) {
            super(cause);
        }
    }

    /**
     * Get an array of Params by name. See createParam(name) implementations for details.
     *
     * @param names parameter names
     * @return A newly-constructed array of newly-constructed Params.
     * @throws QueryScope.MissingVariableException If any of the named scope variables does not exist.
     */
    default QueryScopeParam<?>[] getParams(final Collection<String> names) throws MissingVariableException {
        final QueryScopeParam<?>[] result = new QueryScopeParam[names.size()];
        int pi = 0;
        for (final String name : names) {
            result[pi++] = createParam(name);
        }
        return result;
    }

    /**
     * Get all known scope variable names.
     *
     * @return A caller-owned mutable collection of scope variable names.
     */
    Set<String> getParamNames();

    /**
     * Check if the scope has the given name.
     *
     * @param name param name
     * @return True iff the scope has the given param name
     */
    boolean hasParamName(String name);

    /**
     * Get a QueryScopeParam by name.
     *
     * @param name parameter name
     * @return newly-constructed QueryScopeParam (name + value-snapshot pair).
     * @throws QueryScope.MissingVariableException If any of the named scope variables does not exist.
     */
    <T> QueryScopeParam<T> createParam(final String name) throws MissingVariableException;

    /**
     * Get the value of a given scope parameter by name. Callers may want to unwrap language-specific values using
     * {@link #unwrapObject(Object)} before using them.
     *
     * @param name parameter name.
     * @return parameter value.
     * @throws QueryScope.MissingVariableException If no such scope parameter exists.
     */
    <T> T readParamValue(final String name) throws MissingVariableException;

    /**
     * Get the value of a given scope parameter by name. Callers may want to unwrap language-specific values using
     * {@link #unwrapObject(Object)} before using them.
     *
     * @param name parameter name.
     * @param defaultValue default parameter value.
     * @return parameter value, or the default parameter value, if the value is not present.
     */
    <T> T readParamValue(final String name, final T defaultValue);

    /**
     * Add a parameter to the scope. Objects passed in will have their liveness managed by the scope.
     *
     * @param name parameter name.
     * @param value parameter value.
     */
    <T> void putParam(final String name, final T value);

    @FunctionalInterface
    interface ParamFilter<T> {
        boolean accept(String name, T value);
    }

    /**
     * Returns a mutable map with all objects in the scope. Callers may want to unwrap language-specific values using
     * {@link #unwrapObject(Object)} before using them. This returned map is owned by the caller.
     *
     * @param filter a predicate to filter the map entries
     * @return a caller-owned mutable map with all known variables and their values.
     */
    Map<String, Object> toMap(@NotNull ParamFilter<Object> filter);

    /**
     * Returns a mutable map with all objects in the scope.
     * <p>
     * Callers may want to pass in a valueMapper of {@link #unwrapObject(Object)} which would unwrap values before
     * filtering. The returned map is owned by the caller.
     *
     * @param valueMapper a function to map the values
     * @param filter a predicate to filter the map entries
     * @return a caller-owned mutable map with all known variables and their values.
     * @param <T> the type of the mapped values
     */
    <T> Map<String, T> toMap(
            @NotNull Function<Object, T> valueMapper, @NotNull ParamFilter<T> filter);

    /**
     * Removes any wrapping that exists on a scope param object so that clients can fetch them. Defaults to returning
     * the object itself.
     *
     * @param object the scoped object
     * @return an obj which can be consumed by a client
     */
    default Object unwrapObject(@Nullable Object object) {
        return object;
    }

    @Override
    default LogOutput append(@NotNull final LogOutput logOutput) {
        logOutput.append('{');
        for (final Map.Entry<String, Object> param : toMap((name, value) -> true).entrySet()) {
            logOutput.nl().append(param.getKey()).append("=");
            Object paramValue = param.getValue();
            if (paramValue == this) {
                logOutput.append("this QueryScope (" + paramValue.getClass().getName() + ':'
                        + System.identityHashCode(paramValue) + ')');
            } else if (paramValue instanceof LogOutputAppendable) {
                logOutput.append((LogOutputAppendable) paramValue);
            } else {
                logOutput.append(Objects.toString(paramValue));
            }
        }
        return logOutput.nl().append('}');
    }
}
