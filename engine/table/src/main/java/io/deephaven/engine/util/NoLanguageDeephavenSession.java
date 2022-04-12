/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.util.AbstractScriptSession.Snapshot;
import io.deephaven.engine.util.scripts.ScriptPathLoader;
import io.deephaven.engine.util.scripts.ScriptPathLoaderState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * ScriptSession implementation that simply allows variables to be exported. This is not intended for use in user
 * scripts.
 */
public class NoLanguageDeephavenSession extends AbstractScriptSession<Snapshot> implements ScriptSession {
    private static final String SCRIPT_TYPE = "NoLanguage";

    private final String scriptType;
    private final Map<String, Object> variables;

    public NoLanguageDeephavenSession() {
        this(SCRIPT_TYPE);
    }

    public NoLanguageDeephavenSession(final String scriptType) {
        super(null, null, false);

        this.scriptType = scriptType;
        variables = new LinkedHashMap<>();
    }

    @Override
    public QueryScope newQueryScope() {
        return new SynchronizedScriptSessionQueryScope(this);
    }

    @NotNull
    @Override
    public Object getVariable(String name) throws QueryScope.MissingVariableException {
        final Object var = variables.get(name);
        if (var != null) {
            return var;
        }
        throw new QueryScope.MissingVariableException("No global variable for: " + name);
    }

    @Override
    public <T> T getVariable(String name, T defaultValue) {
        try {
            // noinspection unchecked
            return (T) getVariable(name);
        } catch (QueryScope.MissingVariableException e) {
            return defaultValue;
        }
    }

    @Override
    protected Snapshot emptySnapshot() {
        throw new UnsupportedOperationException(SCRIPT_TYPE + " session does not support emptySnapshot");
    }

    @Override
    protected Snapshot takeSnapshot() {
        throw new UnsupportedOperationException(SCRIPT_TYPE + " session does not support takeSnapshot");
    }

    @Override
    protected Changes createDiff(Snapshot from, Snapshot to, RuntimeException e) {
        throw new UnsupportedOperationException(SCRIPT_TYPE + " session does not support createDiff");
    }

    @Override
    protected void evaluate(String command, @Nullable String scriptName) {
        if (!scriptType.equals(SCRIPT_TYPE)) {
            throw new UnsupportedOperationException(scriptType + " session mode is not enabled");
        }
        throw new UnsupportedOperationException(SCRIPT_TYPE + " session does not support evaluate");
    }

    @Override
    public Map<String, Object> getVariables() {
        return Collections.unmodifiableMap(variables);
    }

    @Override
    public Set<String> getVariableNames() {
        return Collections.unmodifiableSet(variables.keySet());
    }

    @Override
    public boolean hasVariableName(String name) {
        return variables.containsKey(name);
    }

    @Override
    public void setVariable(String name, @Nullable Object newValue) {
        Object oldValue = getVariable(name, null);
        variables.put(name, newValue);
        notifyVariableChange(name, oldValue, newValue);
    }

    @Override
    public String scriptType() {
        return SCRIPT_TYPE;
    }

    @Override
    public void onApplicationInitializationBegin(Supplier<ScriptPathLoader> pathLoader,
            ScriptPathLoaderState scriptLoaderState) {}

    @Override
    public void onApplicationInitializationEnd() {}

    @Override
    public void setScriptPathLoader(Supplier<ScriptPathLoader> scriptPathLoader, boolean caching) {
        throw new UnsupportedOperationException(
                SCRIPT_TYPE + " session does not support setUseOriginalScriptLoaderState");
    }

    @Override
    public void clearScriptPathLoader() {
        throw new UnsupportedOperationException(
                SCRIPT_TYPE + " session does not support setUseOriginalScriptLoaderState");
    }

    @Override
    public boolean setUseOriginalScriptLoaderState(boolean useOriginal) {
        throw new UnsupportedOperationException(
                SCRIPT_TYPE + " session does not support setUseOriginalScriptLoaderState");
    }
}
