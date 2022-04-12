/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import com.github.f4b6a3.uuid.UuidCreator;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.util.NameValidator;
import io.deephaven.base.FileUtils;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableMap;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.table.lang.QueryScopeParam;
import io.deephaven.engine.util.AbstractScriptSession.Snapshot;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.deephaven.engine.table.Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE;
import static io.deephaven.engine.table.Table.NON_DISPLAY_TABLE;

/**
 * This class exists to make all script sessions to be liveness artifacts, and provide a default implementation for
 * evaluateScript which handles liveness and diffs in a consistent way.
 */
public abstract class AbstractScriptSession<S extends Snapshot> extends LivenessScope
        implements ScriptSession, VariableProvider {

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

    private final ObjectTypeLookup objectTypeLookup;
    private final Listener changeListener;

    protected AbstractScriptSession(ObjectTypeLookup objectTypeLookup, @Nullable Listener changeListener,
            boolean isDefaultScriptSession) {
        this.objectTypeLookup = objectTypeLookup;
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

    protected synchronized void publishInitial() {
        applyDiff(emptySnapshot(), takeSnapshot(), null);
    }

    interface Snapshot {

    }

    protected abstract S emptySnapshot();

    protected abstract S takeSnapshot();

    protected abstract Changes createDiff(S from, S to, RuntimeException e);

    protected Changes applyDiff(S from, S to, RuntimeException e) {
        final Changes diff = createDiff(from, to, e);
        if (changeListener != null) {
            changeListener.onScopeChanges(this, diff);
        }
        return diff;
    }

    @Override
    public synchronized final Changes evaluateScript(final String script, final @Nullable String scriptName) {
        RuntimeException evaluateErr = null;
        final S fromSnapshot = takeSnapshot();

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
            evaluateErr = err;
        } finally {
            // restore pointers to query scope static variables
            QueryScope.setScope(prevQueryScope);
            CompilerTools.setContext(prevCompilerContext);
            QueryLibrary.setLibrary(prevQueryLibrary);
        }

        final S toSnapshot = takeSnapshot();

        final Changes diff = applyDiff(fromSnapshot, toSnapshot, evaluateErr);

        // re-throw any captured exception now that our listener knows what query scope state had changed prior
        // to the script session execution error
        if (evaluateErr != null) {
            throw evaluateErr;
        }

        return diff;
    }

    protected void applyVariableChangeToDiff(final Changes diff, String name,
            @Nullable Object fromValue, @Nullable Object toValue) {
        if (fromValue == toValue) {
            return;
        }
        final String fromTypeName = getTypeNameIfDisplayable(fromValue).orElse(null);
        if (fromTypeName == null) {
            fromValue = null;
        }
        final String toTypeName = getTypeNameIfDisplayable(toValue).orElse(null);
        if (toTypeName == null) {
            toValue = null;
        }
        if (fromValue == toValue) {
            return;
        }
        if (fromValue == null) {
            diff.created.put(name, toTypeName);
            return;
        }
        if (toValue == null) {
            diff.removed.put(name, fromTypeName);
            return;
        }
        if (!fromTypeName.equals(toTypeName)) {
            diff.created.put(name, toTypeName);
            diff.removed.put(name, fromTypeName);
            return;
        }
        diff.updated.put(name, toTypeName);
    }

    private Optional<String> getTypeNameIfDisplayable(Object object) {
        if (object == null) {
            return Optional.empty();
        }
        // Should this be consolidated down into TypeLookup and brought into engine?
        if (object instanceof Table) {
            final Table table = (Table) object;
            if (table.hasAttribute(NON_DISPLAY_TABLE)) {
                return Optional.empty();
            }
            if (table.hasAttribute(HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE)) {
                return Optional.of("TreeTable");
            }
            return Optional.of("Table");
        }
        if (object instanceof TableMap) {
            return Optional.empty();
            // TODO(deephaven-core#1762): Implement Smart-Keys and Table-Maps
            // return Optional.of("TableMap");
        }
        return objectTypeLookup.findObjectType(object).map(ObjectType::name);
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
