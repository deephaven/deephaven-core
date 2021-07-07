package io.deephaven.db.util;

import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.util.scripts.ScriptPathLoader;
import io.deephaven.db.util.scripts.ScriptPathLoaderState;
import io.deephaven.lang.parse.api.Languages;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * ScriptSession implementation that simply allows variables to be exported. This is not intended for use in user scripts.
 */
public class NoLanguageDeephavenSession extends AbstractScriptSession implements ScriptSession {

    private final String scriptType;
    private final Map<String, Object> variables;

    public NoLanguageDeephavenSession() {
        this(Languages.LANGUAGE_OTHER);
    }

    public NoLanguageDeephavenSession(final String scriptType) {
        super(false);

        this.scriptType = scriptType;
        variables = new LinkedHashMap<>();
    }

    @Override
    public QueryScope newQueryScope() {
        return new QueryScope.SynchronizedScriptSessionImpl(this);
    }

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
            //noinspection unchecked
            return (T)getVariable(name);
        } catch (QueryScope.MissingVariableException e) {
            return defaultValue;
        }
    }

    @Override
    protected void evaluate(String command, @Nullable String scriptName) {
        if (!scriptType.equals(Languages.LANGUAGE_OTHER)) {
            throw new UnsupportedOperationException(scriptType + " session mode is not enabled");
        }
        throw new UnsupportedOperationException(Languages.LANGUAGE_OTHER + " session does not support evaluate");
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
    public void setVariable(String name, Object value) {
        variables.put(name, value);
    }

    @Override
    public String scriptType() {
        return scriptType;
    }

    @Override
    public void onApplicationInitializationBegin(Supplier<ScriptPathLoader> pathLoader, ScriptPathLoaderState scriptLoaderState) {
    }

    @Override
    public void onApplicationInitializationEnd() {
    }

    @Override
    public void setScriptPathLoader(Supplier<ScriptPathLoader> scriptPathLoader, boolean caching) {
        throw new UnsupportedOperationException(scriptType + " session does not support setUseOriginalScriptLoaderState");
    }

    @Override
    public void clearScriptPathLoader() {
        throw new UnsupportedOperationException(scriptType + " session does not support setUseOriginalScriptLoaderState");
    }

    @Override
    public boolean setUseOriginalScriptLoaderState(boolean useOriginal) {
        throw new UnsupportedOperationException(scriptType + " session does not support setUseOriginalScriptLoaderState");
    }
}
