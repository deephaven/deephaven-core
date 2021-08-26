/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.select;

import io.deephaven.base.CompareUtils;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.db.tables.utils.*;
import io.deephaven.db.util.ScriptSession;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Variable scope used to resolve parameter values during query execution.
 */
public abstract class QueryScope implements LogOutputAppendable {

    // -----------------------------------------------------------------------------------------------------------------
    // Singleton Management (ThreadLocal eliminated for the time being)
    // -----------------------------------------------------------------------------------------------------------------

    private static volatile QueryScope defaultScope = null;
    private static final ThreadLocal<QueryScope> currentScope =
        ThreadLocal.withInitial(QueryScope::getDefaultScope);

    private static QueryScope getDefaultScope() {
        if (defaultScope == null) {
            synchronized (QueryScope.class) {
                if (defaultScope == null) {
                    defaultScope = new StandaloneImpl();
                }
            }
        }
        return defaultScope;
    }

    /**
     * Sets the default scope.
     *
     * @param scope the script session's query scope
     * @throws IllegalStateException if default scope is already set
     * @throws NullPointerException if scope is null
     */
    public static synchronized void setDefaultScope(final QueryScope scope) {
        if (defaultScope != null) {
            throw new IllegalStateException(
                "It's too late to set default scope; it's already set to: " + defaultScope);
        }
        defaultScope = Objects.requireNonNull(scope);
    }

    /**
     * Sets the default {@link QueryScope} to be used in the current context. By default there is a
     * {@link StandaloneImpl} created by the static initializer and set as the defaultInstance. The
     * method allows the use of a new or separate instance as the default instance for static
     * methods.
     *
     * @param queryScope {@link QueryScope} to set as the new default instance; null clears the
     *        scope.
     */
    public static synchronized void setScope(final QueryScope queryScope) {
        if (queryScope == null) {
            currentScope.remove();
        } else {
            currentScope.set(queryScope);
        }
    }

    /**
     * Retrieve the default {@link QueryScope} instance which will be used by static methods.
     *
     * @return {@link QueryScope}
     */
    public static QueryScope getScope() {
        return currentScope.get();
    }

    /**
     * Adds a parameter to the default instance {@link QueryScope}, or updates the value of an
     * existing parameter.
     *
     * @param name String name of the parameter to add.
     * @param value value to assign to the parameter.
     * @param <T> type of the parameter/value.
     */
    public static <T> void addParam(final String name, final T value) {
        getScope().putParam(name, value);
    }

    /**
     * Adds an object's declared fields to the scope.
     *
     * @param object object whose fields will be added.
     */
    public static void addObjectFields(final Object object) {
        getScope().putObjectFields(object);
    }

    /**
     * Gets a parameter from the default instance {@link QueryScope}.
     *
     * @param name parameter name.
     * @param <T> parameter type.
     * @return parameter value.
     * @throws MissingVariableException variable name is not defined.
     */
    public static <T> T getParamValue(final String name) throws MissingVariableException {
        return getScope().readParamValue(name);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Implementation
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Parameter name for the query name.
     */
    public static final String QUERY_NAME_PARAM_NAME = "__QUERY_NAME__";

    private volatile String queryNameValue;

    /**
     * A type of RuntimeException thrown when a variable referenced within the {@link QueryScope} is
     * not defined or, more likely, has not been added to the scope.
     */
    public static class MissingVariableException extends RuntimeException {

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
     * Apply conversions to certain scope variable values.
     * 
     * @param value value
     * @return value, or an appropriately converted substitute.
     */
    private static Object applyValueConversions(final Object value) {
        if (value instanceof String) {
            final String stringValue = (String) value;

            if (stringValue.length() > 0 && stringValue.charAt(0) == '\''
                && stringValue.charAt(stringValue.length() - 1) == '\'') {
                final String datetimeString = stringValue.substring(1, stringValue.length() - 1);

                final DBDateTime dateTime = DBTimeUtils.convertDateTimeQuiet(datetimeString);
                if (dateTime != null) {
                    return dateTime;
                }

                final long localTime = DBTimeUtils.convertTimeQuiet(datetimeString);
                if (localTime != QueryConstants.NULL_LONG) {
                    return localTime;
                }

                final DBPeriod period = DBTimeUtils.convertPeriodQuiet(datetimeString);
                if (period != null) {
                    return period;
                }

                throw new RuntimeException("Cannot parse datetime/time/period : " + stringValue);
            }
        }

        return value;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Scope manipulation helper methods
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Get an array of Params by name. See createParam(name) implementations for details.
     *
     * @param names parameter names
     * @return A newly-constructed array of newly-constructed Params.
     * @throws io.deephaven.db.tables.select.QueryScope.MissingVariableException If any of the named
     *         scope variables does not exist.
     */
    public final Param[] getParams(final Collection<String> names) throws MissingVariableException {
        final Param[] result = new Param[names.size()];
        int pi = 0;
        for (final String name : names) {
            result[pi++] = createParam(name);
        }
        return result;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // General scope manipulation methods
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Get all known scope variable names.
     *
     * @return A collection of scope variable names.
     */
    public abstract Set<String> getParamNames();

    /**
     * Check if the scope has the given name.
     *
     * @param name param name
     * @return True iff the scope has the given param name
     */
    public abstract boolean hasParamName(String name);

    /**
     * Get a Param by name.
     * 
     * @param name parameter name
     * @return newly-constructed Param (name + value-snapshot pair).
     * @throws io.deephaven.db.tables.select.QueryScope.MissingVariableException If any of the named
     *         scope variables does not exist.
     */
    protected abstract <T> Param<T> createParam(final String name) throws MissingVariableException;

    /**
     * Get the value of a given scope parameter by name.
     *
     * @param name parameter name.
     * @return parameter value.
     * @throws io.deephaven.db.tables.select.QueryScope.MissingVariableException If no such scope
     *         parameter exists.
     */
    public abstract <T> T readParamValue(final String name) throws MissingVariableException;

    /**
     * Get the value of a given scope parameter by name.
     *
     * @param name parameter name.
     * @param defaultValue default parameter value.
     * @return parameter value, or the default parameter value, if the value is not present.
     */
    public abstract <T> T readParamValue(final String name, final T defaultValue);

    /**
     * Add a parameter to the scope.
     *
     * @param name parameter name.
     * @param value parameter value.
     */
    public abstract <T> void putParam(final String name, final T value);

    /**
     * Add an object's public members (referenced reflectively, not a shallow copy!) to this scope
     * if supported. <b>Note:</b> This is an optional method.
     *
     * @param object object to add public members from.
     */
    public abstract void putObjectFields(final Object object);

    // -----------------------------------------------------------------------------------------------------------------
    // Accessors for "special" scope variables
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Gets the query name.
     *
     * @return query name.
     */
    public final String getQueryName() {
        return queryNameValue;
    }

    /**
     * Sets the query name.
     *
     * @param queryName query name.
     */
    public void setQueryName(String queryName) {
        if (CompareUtils.equals(queryName, queryNameValue)) {
            return;
        }
        putParam(QUERY_NAME_PARAM_NAME, queryName);
        queryNameValue = queryName;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LogOutputAppendable implementation
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        logOutput.append('{');
        for (final String paramName : getParamNames()) {
            final Object paramValue = readParamValue(paramName);
            logOutput.nl().append(paramName).append("=");
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

    // -----------------------------------------------------------------------------------------------------------------
    // Map-based implementation, with remote scope and object reflection support
    // -----------------------------------------------------------------------------------------------------------------

    public static class StandaloneImpl extends QueryScope {

        private final KeyedObjectHashMap<String, ValueRetriever> valueRetrievers =
            new KeyedObjectHashMap<>(new ValueRetrieverNameKey());

        public StandaloneImpl() {}

        @Override
        public Set<String> getParamNames() {
            return valueRetrievers.keySet();
        }

        @Override
        public boolean hasParamName(String name) {
            return valueRetrievers.containsKey(name);
        }

        @Override
        protected <T> Param<T> createParam(final String name) throws MissingVariableException {
            // noinspection unchecked
            final ValueRetriever<T> valueRetriever = valueRetrievers.get(name);
            if (valueRetriever == null) {
                throw new MissingVariableException("Missing variable " + name);
            }
            return valueRetriever.createParam();
        }

        @Override
        public <T> T readParamValue(final String name) throws MissingVariableException {
            // noinspection unchecked
            final ValueRetriever<T> valueRetriever = valueRetrievers.get(name);
            if (valueRetriever == null) {
                throw new MissingVariableException("Missing variable " + name);
            }
            return valueRetriever.getValue();
        }

        @Override
        public <T> T readParamValue(final String name, final T defaultValue) {
            // noinspection unchecked
            final ValueRetriever<T> valueRetriever = valueRetrievers.get(name);
            if (valueRetriever == null) {
                return defaultValue;
            }
            return valueRetriever.getValue();
        }

        @Override
        public <T> void putParam(final String name, final T value) {
            NameValidator.validateQueryParameterName(name);
            // TODO: Can I get rid of this applyValueConversions? It's too inconsistent to feel
            // safe.
            valueRetrievers.put(name,
                new SimpleValueRetriever<>(name, applyValueConversions(value)));
        }

        public void putObjectFields(final Object object) {
            for (final Field field : object.getClass().getDeclaredFields()) {
                valueRetrievers.put(field.getName(), new ReflectiveValueRetriever(object, field));
            }
        }

        private static abstract class ValueRetriever<T> {

            private final String name;

            protected ValueRetriever(String name) {
                this.name = name;
            }

            public String getName() {
                return name;
            }

            public abstract T getValue();

            public abstract Class<T> getType();

            public abstract Param<T> createParam();
        }

        private static class ValueRetrieverNameKey
            extends KeyedObjectKey.Basic<String, ValueRetriever> {

            @Override
            public String getKey(ValueRetriever valueRetriever) {
                return valueRetriever.getName();
            }
        }

        private static class SimpleValueRetriever<T> extends ValueRetriever<T> {

            private final T value;

            public SimpleValueRetriever(final String name, final T value) {
                super(name);
                this.value = value;
            }

            @Override
            public T getValue() {
                return value;
            }

            @Override
            public Class<T> getType() {
                // noinspection unchecked
                return (Class<T>) (value != null ? value.getClass() : Object.class);
            }

            @Override
            public Param<T> createParam() {
                return new Param<>(getName(), getValue());
            }
        }

        private static class ReflectiveValueRetriever<T> extends ValueRetriever<T> {

            private final Object object;
            private final Field field;

            public ReflectiveValueRetriever(final Object object, final Field field) {
                super(field.getName());
                this.object = object;
                this.field = field;
                field.setAccessible(true);
            }

            @Override
            public T getValue() {
                try {
                    // noinspection unchecked
                    return (T) field.get(object);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Class<T> getType() {
                // noinspection unchecked
                return (Class<T>) field.getType();
            }

            @Override
            public Param<T> createParam() {
                return new Param<>(getName(), getValue());
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ScriptSession-based implementation, with no remote scope or object reflection support
    // -----------------------------------------------------------------------------------------------------------------

    public static class SynchronizedScriptSessionImpl extends BaseScriptSessionImpl {
        public SynchronizedScriptSessionImpl(@NotNull final ScriptSession scriptSession) {
            super(scriptSession);
        }

        @Override
        public synchronized Set<String> getParamNames() {
            return scriptSession.getVariableNames();
        }

        @Override
        public boolean hasParamName(String name) {
            return scriptSession.hasVariableName(name);
        }

        @Override
        protected synchronized <T> Param<T> createParam(final String name)
            throws MissingVariableException {
            // noinspection unchecked
            return new Param<>(name, (T) scriptSession.getVariable(name));
        }

        @Override
        public synchronized <T> T readParamValue(final String name)
            throws MissingVariableException {
            // noinspection unchecked
            return (T) scriptSession.getVariable(name);
        }

        @Override
        public synchronized <T> T readParamValue(final String name, final T defaultValue) {
            return scriptSession.getVariable(name, defaultValue);
        }

        @Override
        public synchronized <T> void putParam(final String name, final T value) {
            scriptSession.setVariable(NameValidator.validateQueryParameterName(name), value);
        }
    }

    public static class UnsynchronizedScriptSessionImpl extends BaseScriptSessionImpl {
        public UnsynchronizedScriptSessionImpl(@NotNull final ScriptSession scriptSession) {
            super(scriptSession);
        }

        @Override
        public Set<String> getParamNames() {
            return scriptSession.getVariableNames();
        }

        @Override
        public boolean hasParamName(String name) {
            return scriptSession.hasVariableName(name);
        }

        @Override
        protected <T> Param<T> createParam(final String name) throws MissingVariableException {
            // noinspection unchecked
            return new Param<>(name, (T) scriptSession.getVariable(name));
        }

        @Override
        public <T> T readParamValue(final String name) throws MissingVariableException {
            // noinspection unchecked
            return (T) scriptSession.getVariable(name);
        }

        @Override
        public <T> T readParamValue(final String name, final T defaultValue) {
            return scriptSession.getVariable(name, defaultValue);
        }

        @Override
        public <T> void putParam(final String name, final T value) {
            scriptSession.setVariable(NameValidator.validateQueryParameterName(name), value);
        }
    }

    private abstract static class BaseScriptSessionImpl extends QueryScope {
        final ScriptSession scriptSession;

        private BaseScriptSessionImpl(ScriptSession scriptSession) {
            this.scriptSession = scriptSession;
        }

        @Override
        public void putObjectFields(Object object) {
            throw new UnsupportedOperationException();
        }
    }
}
