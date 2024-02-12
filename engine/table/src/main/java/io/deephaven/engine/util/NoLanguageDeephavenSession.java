/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * ScriptSession implementation that simply allows variables to be exported. This is not intended for use in user
 * scripts.
 */
public class NoLanguageDeephavenSession extends AbstractScriptSession<AbstractScriptSession.Snapshot> {
    private static final String SCRIPT_TYPE = "NoLanguage";

    private final String scriptType;
    private final Map<String, Object> variables;

    public NoLanguageDeephavenSession(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer) {
        this(updateGraph, operationInitializer, SCRIPT_TYPE);
    }

    public NoLanguageDeephavenSession(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer,
            final String scriptType) {
        super(updateGraph, operationInitializer, null, null);

        this.scriptType = scriptType;
        variables = Collections.synchronizedMap(new LinkedHashMap<>());
    }

    @Override
    protected <T> T getVariable(String name) {
        synchronized (variables) {
            if (!variables.containsKey(name)) {
                throw new QueryScope.MissingVariableException("Missing variable " + name);
            }
            // noinspection unchecked
            return (T) variables.get(name);
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
    protected Set<String> getVariableNames(Predicate<String> allowName) {
        synchronized (variables) {
            return variables.keySet().stream().filter(allowName).collect(Collectors.toUnmodifiableSet());
        }
    }

    @Override
    protected boolean hasVariable(String name) {
        return variables.containsKey(name);
    }

    @Override
    protected Object setVariable(String name, @Nullable Object newValue) {
        return variables.put(name, newValue);
        // changeListener is always null for NoLanguageDeephavenSession; we have no mechanism for reporting scope
        // changes
    }

    @Override
    protected Map<String, Object> getAllValues(@NotNull final Predicate<Map.Entry<String, Object>> predicate) {
        synchronized (variables) {
            return variables.entrySet().stream()
                    .filter(predicate)
                    .collect(HashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), HashMap::putAll);
        }
    }

    @Override
    public String scriptType() {
        return SCRIPT_TYPE;
    }
}
