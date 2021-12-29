/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import com.github.f4b6a3.uuid.UuidCreator;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.util.NameValidator;
import io.deephaven.base.FileUtils;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScopeParam;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * This class exists to make all script sessions to be liveness artifacts, and provide a default implementation for
 * evaluateScript which handles liveness and diffs in a consistent way.
 */
public abstract class AbstractScriptSession extends LivenessScope implements ScriptSession, VariableProvider {

    private static final Path CLASS_CACHE_LOCATION = CacheDir.get().resolve("script-session-classes");

    public static void createScriptCache() {
        final File classCacheDirectory = CLASS_CACHE_LOCATION.toFile();
        createOrClearDirectory(classCacheDirectory);
    }

    private static void createOrClearDirectory(final File directory) {
        if (directory.exists()) {
            FileUtils.deleteRecursively(directory);
        }
        if (!directory.mkdirs()) {
            throw new UncheckedDeephavenException(
                    "Failed to create class cache directory " + directory.getAbsolutePath());
        }
    }

    private final File classCacheDirectory;

    protected final QueryScope queryScope;
    protected final QueryLibrary queryLibrary;
    protected final CompilerTools.Context compilerContext;

    private final Listener changeListener;

    protected AbstractScriptSession(@Nullable Listener changeListener, boolean isDefaultScriptSession) {
        this.changeListener = changeListener;

        // TODO(deephaven-core#1713): Introduce instance-id concept
        final UUID scriptCacheId = UuidCreator.getRandomBased();
        classCacheDirectory = CLASS_CACHE_LOCATION.resolve(UuidCreator.toString(scriptCacheId)).toFile();
        createOrClearDirectory(classCacheDirectory);

        queryScope = newQueryScope();
        queryLibrary = QueryLibrary.makeNewLibrary();

        compilerContext = new CompilerTools.Context(classCacheDirectory, getClass().getClassLoader()) {
            {
                addClassSource(getFakeClassDestination());
            }

            @Override
            public File getFakeClassDestination() {
                return classCacheDirectory;
            }

            @Override
            public String getClassPath() {
                return classCacheDirectory.getAbsolutePath() + File.pathSeparatorChar + super.getClassPath();
            }
        };

        if (isDefaultScriptSession) {
            CompilerTools.setDefaultContext(compilerContext);
            QueryScope.setDefaultScope(queryScope);
            QueryLibrary.setDefaultLibrary(queryLibrary);
        }
    }

    @Override
    public synchronized final Changes evaluateScript(final String script, final @Nullable String scriptName) {
        final Changes diff = new Changes();
        final Map<String, Object> existingScope = new HashMap<>(getVariables());

        // store pointers to exist query scope static variables
        final QueryLibrary prevQueryLibrary = QueryLibrary.getLibrary();
        final CompilerTools.Context prevCompilerContext = CompilerTools.getContext();
        final QueryScope prevQueryScope = QueryScope.getScope();

        // retain any objects which are created in the executed code, we'll release them when the script session closes
        try (final SafeCloseable ignored = LivenessScopeStack.open(this, false)) {
            // point query scope static state to our session's state
            QueryScope.setScope(queryScope);
            CompilerTools.setContext(compilerContext);
            QueryLibrary.setLibrary(queryLibrary);

            // actually evaluate the script
            evaluate(script, scriptName);
        } catch (final RuntimeException err) {
            diff.error = err;
        } finally {
            // restore pointers to query scope static variables
            QueryScope.setScope(prevQueryScope);
            CompilerTools.setContext(prevCompilerContext);
            QueryLibrary.setLibrary(prevQueryLibrary);
        }

        final Map<String, Object> newScope = new HashMap<>(getVariables());

        // produce a diff
        for (final Map.Entry<String, Object> entry : newScope.entrySet()) {
            final String name = entry.getKey();
            final Object existingValue = existingScope.get(name);
            final Object newValue = entry.getValue();
            applyVariableChangeToDiff(diff, name, existingValue, newValue);
        }

        for (final Map.Entry<String, Object> entry : existingScope.entrySet()) {
            final String name = entry.getKey();
            if (newScope.containsKey(name)) {
                continue; // this is already handled even if old or new values are non-displayable
            }
            applyVariableChangeToDiff(diff, name, entry.getValue(), null);
        }

        if (changeListener != null && !diff.isEmpty()) {
            changeListener.onScopeChanges(this, diff);
        }

        // re-throw any captured exception now that our listener knows what query scope state had changed prior
        // to the script session execution error
        if (diff.error != null) {
            throw diff.error;
        }

        return diff;
    }

    private void applyVariableChangeToDiff(final Changes diff, String name,
            @Nullable Object fromValue, @Nullable Object toValue) {
        fromValue = unwrapObject(fromValue);
        final ExportedObjectType fromType = ExportedObjectType.fromObject(fromValue);
        if (!fromType.isDisplayable()) {
            fromValue = null;
        }
        toValue = unwrapObject(toValue);
        final ExportedObjectType toType = ExportedObjectType.fromObject(toValue);
        if (!toType.isDisplayable()) {
            toValue = null;
        }
        if (fromValue == toValue) {
            return;
        }

        if (fromValue == null) {
            diff.created.put(name, toType);
        } else if (toValue == null) {
            diff.removed.put(name, fromType);
        } else if (fromType != toType) {
            diff.created.put(name, toType);
            diff.removed.put(name, fromType);
        } else {
            diff.updated.put(name, toType);
        }
    }

    @Override
    public Changes evaluateScript(Path scriptPath) {
        try {
            final String script = FileUtils.readTextFile(scriptPath.toFile());
            return evaluateScript(script, scriptPath.toString());
        } catch (IOException err) {
            throw new UncheckedDeephavenException(
                    String.format("could not read script file %s: ", scriptPath.toString()), err);
        }
    }

    protected synchronized void notifyVariableChange(String name, @Nullable Object oldValue,
            @Nullable Object newValue) {
        if (changeListener == null) {
            return;
        }

        Changes changes = new Changes();
        applyVariableChangeToDiff(changes, name, oldValue, newValue);
        if (!changes.isEmpty()) {
            changeListener.onScopeChanges(this, changes);
        }
    }

    @Override
    protected void destroy() {
        super.destroy();
        // Clear our session's script directory:
        if (classCacheDirectory.exists()) {
            FileUtils.deleteRecursively(classCacheDirectory);
        }
    }

    /**
     * Evaluates command in the context of the current ScriptSession.
     *
     * @param command the command to evaluate
     * @param scriptName an optional script name, which may be ignored by the implementation, or used improve error
     *        messages or for other internal purposes
     */
    protected abstract void evaluate(String command, @Nullable String scriptName);

    /**
     * @return a query scope for this session; only invoked during construction
     */
    protected abstract QueryScope newQueryScope();

    @Override
    public Class getVariableType(final String var) {
        final Object result = getVariable(var, null);
        if (result == null) {
            return null;
        } else if (result instanceof Table) {
            return Table.class;
        } else {
            return result.getClass();
        }
    }


    @Override
    public TableDefinition getTableDefinition(final String var) {
        Object o = getVariable(var, null);
        return o instanceof Table ? ((Table) o).getDefinition() : null;
    }

    @Override
    public VariableProvider getVariableProvider() {
        return this;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ScriptSession-based QueryScope implementation, with no remote scope or object reflection support
    // -----------------------------------------------------------------------------------------------------------------

    private abstract static class ScriptSessionQueryScope extends QueryScope {
        final ScriptSession scriptSession;

        private ScriptSessionQueryScope(ScriptSession scriptSession) {
            this.scriptSession = scriptSession;
        }

        @Override
        public void putObjectFields(Object object) {
            throw new UnsupportedOperationException();
        }
    }

    public static class SynchronizedScriptSessionQueryScope extends ScriptSessionQueryScope {
        public SynchronizedScriptSessionQueryScope(@NotNull final ScriptSession scriptSession) {
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
        protected synchronized <T> QueryScopeParam<T> createParam(final String name)
                throws QueryScope.MissingVariableException {
            // noinspection unchecked
            return new QueryScopeParam<>(name, (T) scriptSession.getVariable(name));
        }

        @Override
        public synchronized <T> T readParamValue(final String name) throws QueryScope.MissingVariableException {
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

    public static class UnsynchronizedScriptSessionQueryScope extends ScriptSessionQueryScope {
        public UnsynchronizedScriptSessionQueryScope(@NotNull final ScriptSession scriptSession) {
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
        protected <T> QueryScopeParam<T> createParam(final String name) throws QueryScope.MissingVariableException {
            // noinspection unchecked
            return new QueryScopeParam<>(name, (T) scriptSession.getVariable(name));
        }

        @Override
        public <T> T readParamValue(final String name) throws QueryScope.MissingVariableException {
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
}
