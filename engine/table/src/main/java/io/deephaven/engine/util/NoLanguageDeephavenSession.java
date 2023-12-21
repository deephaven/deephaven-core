/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * ScriptSession implementation that simply allows variables to be exported. This is not intended for use in user
 * scripts.
 */
public class NoLanguageDeephavenSession extends AbstractScriptSession<AbstractScriptSession.Snapshot> {
    private static final String SCRIPT_TYPE = "NoLanguage";

    private final String scriptType;
    private final Map<String, Object> variables;

    public NoLanguageDeephavenSession(final UpdateGraph updateGraph,
            final ThreadInitializationFactory threadInitializationFactory) {
        this(updateGraph, threadInitializationFactory, SCRIPT_TYPE);
    }

    public NoLanguageDeephavenSession(final UpdateGraph updateGraph,
            final ThreadInitializationFactory threadInitializationFactory, final String scriptType) {
        super(updateGraph, threadInitializationFactory, null, null);

        this.scriptType = scriptType;
        variables = new LinkedHashMap<>();
    }

    @Override
    protected <T> T getVariable(String name) throws QueryScope.MissingVariableException {
        if (!variables.containsKey(name)) {
            throw new QueryScope.MissingVariableException("No global variable for: " + name);
        }
        // noinspection unchecked
        return (T) variables.get(name);
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
    protected Set<String> getVariableNames() {
        return Set.copyOf(variables.keySet());
    }

    @Override
    protected boolean hasVariableName(String name) {
        return variables.containsKey(name);
    }

    @Override
    protected void setVariable(String name, @Nullable Object newValue) {
        variables.put(name, newValue);
        // changeListener is always null for NoLanguageDeephavenSession; we have no mechanism for reporting scope
        // changes
    }

    @Override
    public String scriptType() {
        return SCRIPT_TYPE;
    }
}
