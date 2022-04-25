package io.deephaven.engine.util;

import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.util.scripts.ScriptPathLoader;
import io.deephaven.engine.util.scripts.ScriptPathLoaderState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The delegating script session delegates all calls to another script session. When evaluating a script it massages the
 * Changes to the QueryScope so that any modifications that are being seen for the first time by the api-client come
 * across as new entries and not as modified entries.
 */
public class DelegatingScriptSession implements ScriptSession {
    private final ScriptSession delegate;
    private final Set<String> knownVariables = new HashSet<>();

    public DelegatingScriptSession(final ScriptSession delegate) {
        this.delegate = delegate;
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

    @NotNull
    @Override
    public Object getVariable(String name) throws QueryScope.MissingVariableException {
        return delegate.getVariable(name);
    }

    @Override
    public <T> T getVariable(String name, T defaultValue) {
        return delegate.getVariable(name, defaultValue);
    }

    @Override
    public VariableProvider getVariableProvider() {
        return delegate.getVariableProvider();
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
    public Map<String, Object> getVariables() {
        return delegate.getVariables();
    }

    @Override
    public Set<String> getVariableNames() {
        return delegate.getVariableNames();
    }

    @Override
    public boolean hasVariableName(String name) {
        return delegate.hasVariableName(name);
    }

    @Override
    public void setVariable(String name, Object value) {
        delegate.setVariable(name, value);
    }

    @Override
    public String scriptType() {
        return delegate.scriptType();
    }

    @Override
    public void onApplicationInitializationBegin(Supplier<ScriptPathLoader> pathLoader,
            ScriptPathLoaderState scriptLoaderState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onApplicationInitializationEnd() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setScriptPathLoader(Supplier<ScriptPathLoader> scriptPathLoader, boolean caching) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearScriptPathLoader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean setUseOriginalScriptLoaderState(boolean useOriginal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryManage(@NotNull LivenessReferent referent) {
        return delegate.tryManage(referent);
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

    @Override
    public void release() {
        delegate.release();
    }
}
