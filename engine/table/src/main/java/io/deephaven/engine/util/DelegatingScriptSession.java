//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.context.ExecutionContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The delegating script session delegates all calls to another script session. When evaluating a script it massages the
 * Changes to the QueryScope so that any modifications that are being seen for the first time by the api-client come
 * across as new entries and not as modified entries.
 */
public class DelegatingScriptSession implements ScriptSession {
    private final ScriptSession delegate;
    private final Set<String> knownVariables = new HashSet<>();

    public DelegatingScriptSession(final ScriptSession delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    private Changes contextualizeChanges(final Changes diff) {
        knownVariables.removeAll(diff.removed.keySet());

        Set<String> updatesThatAreNew = null;
        for (final String updateKey : diff.updated.keySet()) {
            if (knownVariables.contains(updateKey)) {
                continue;
            }

            if (updatesThatAreNew == null) {
                updatesThatAreNew = new HashSet<>();
            }
            updatesThatAreNew.add(updateKey);
            diff.created.put(updateKey, diff.updated.get(updateKey));
        }

        if (updatesThatAreNew != null) {
            updatesThatAreNew.forEach(diff.updated::remove);
        }
        knownVariables.addAll(diff.created.keySet());
        return diff;
    }

    @Override
    public ExecutionContext getExecutionContext() {
        return delegate.getExecutionContext();
    }

    @Override
    public void observeScopeChanges() {
        delegate.observeScopeChanges();
    }

    @Override
    public QueryScope getQueryScope() {
        return delegate.getQueryScope();
    }

    @Override
    public Changes evaluateScript(String script, @Nullable String scriptName) {
        return contextualizeChanges(delegate.evaluateScript(script, scriptName));
    }

    @Override
    public Changes evaluateScript(Path scriptPath) {
        return contextualizeChanges(delegate.evaluateScript(scriptPath));
    }

    @Override
    public String scriptType() {
        return delegate.scriptType();
    }

    @Override
    public boolean tryManage(@NotNull LivenessReferent referent) {
        return delegate.tryManage(referent);
    }

    @Override
    public boolean tryUnmanage(@NotNull LivenessReferent referent) {
        return delegate.tryUnmanage(referent);
    }

    @Override
    public boolean tryUnmanage(@NotNull Stream<? extends LivenessReferent> referents) {
        return delegate.tryUnmanage(referents);
    }

    @Override
    public boolean tryRetainReference() {
        return delegate.tryRetainReference();
    }

    @Override
    public void dropReference() {
        delegate.dropReference();
    }

    @Override
    public WeakReference<? extends LivenessReferent> getWeakReference() {
        return delegate.getWeakReference();
    }
}
