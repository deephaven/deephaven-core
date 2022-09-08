/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import com.github.f4b6a3.uuid.UuidCreator;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.util.NameValidator;
import io.deephaven.base.FileUtils;
import io.deephaven.configuration.CacheDir;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.deephaven.engine.table.Table.HIERARCHICAL_CHILDREN_TABLE_ATTRIBUTE;
import static io.deephaven.engine.table.Table.NON_DISPLAY_TABLE;

/**
 * This class exists to make all script sessions to be liveness artifacts, and provide a default implementation for
 * evaluateScript which handles liveness and diffs in a consistent way.
 */
public abstract class AbstractScriptSession<S extends AbstractScriptSession.Snapshot> extends LivenessScope
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

    protected final ExecutionContext executionContext;

    private final ObjectTypeLookup objectTypeLookup;
    private final Listener changeListener;

    protected AbstractScriptSession(ObjectTypeLookup objectTypeLookup, @Nullable Listener changeListener) {
        this.objectTypeLookup = objectTypeLookup;
        this.changeListener = changeListener;

        // TODO(deephaven-core#1713): Introduce instance-id concept
        final UUID scriptCacheId = UuidCreator.getRandomBased();
        classCacheDirectory = CLASS_CACHE_LOCATION.resolve(UuidCreator.toString(scriptCacheId)).toFile();
        createOrClearDirectory(classCacheDirectory);

        final QueryScope queryScope = newQueryScope();
        final QueryCompiler compilerContext = QueryCompiler.create(classCacheDirectory, getClass().getClassLoader());

        executionContext = ExecutionContext.newBuilder()
                .markSystemic()
                .newQueryLibrary()
                .setQueryScope(queryScope)
                .setQueryCompiler(compilerContext)
                .build();
    }

    @Override
    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    protected synchronized void publishInitial() {
        try (S empty = emptySnapshot(); S snapshot = takeSnapshot()) {
            applyDiff(empty, snapshot, null);
        }
    }

    protected interface Snapshot extends SafeCloseable {

    }

    @Override
    public synchronized SnapshotScope snapshot(@Nullable SnapshotScope previousIfPresent) {
        // TODO deephaven-core#2453 this should be redone, along with other scope change handling
        if (previousIfPresent != null) {
            previousIfPresent.close();
        }
        S snapshot = takeSnapshot();
        return () -> finishSnapshot(snapshot);
    }

    private synchronized void finishSnapshot(S beforeSnapshot) {
        try (beforeSnapshot; S afterSnapshot = takeSnapshot()) {
            applyDiff(beforeSnapshot, afterSnapshot, null);
        }
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
        final Changes diff;
        // retain any objects which are created in the executed code, we'll release them when the script session
        // closes
        try (S fromSnapshot = takeSnapshot();
                final SafeCloseable ignored = LivenessScopeStack.open(this, false)) {

            try {
                // actually evaluate the script
                executionContext.apply(() -> evaluate(script, scriptName));
            } catch (final RuntimeException err) {
                evaluateErr = err;
            }

            try (S toSnapshot = takeSnapshot()) {
                diff = applyDiff(fromSnapshot, toSnapshot, evaluateErr);
            }
        }

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
            if (table.hasAttribute(HIERARCHICAL_CHILDREN_TABLE_ATTRIBUTE)) {
                return Optional.of("TreeTable");
            }
            return Optional.of("Table");
        }
        if (object instanceof PartitionedTable) {
            return Optional.of("PartitionedTable");
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
    public Class<?> getVariableType(final String var) {
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

    public abstract static class ScriptSessionQueryScope extends QueryScope {
        final ScriptSession scriptSession;

        public ScriptSessionQueryScope(ScriptSession scriptSession) {
            this.scriptSession = scriptSession;
        }

        @Override
        public void putObjectFields(Object object) {
            throw new UnsupportedOperationException();
        }

        public ScriptSession scriptSession() {
            return scriptSession;
        }
    }

    public static class UnsynchronizedScriptSessionQueryScope extends ScriptSessionQueryScope {
        public UnsynchronizedScriptSessionQueryScope(@NotNull final ScriptSession scriptSession) {
            super(scriptSession);
        }

        @Override
        public Set<String> getParamNames() {
            final Set<String> result = new LinkedHashSet<>();
            for (final String name : scriptSession.getVariableNames()) {
                if (NameValidator.isValidQueryParameterName(name)) {
                    result.add(name);
                }
            }
            return result;
        }

        @Override
        public boolean hasParamName(String name) {
            return NameValidator.isValidQueryParameterName(name) && scriptSession.hasVariableName(name);
        }

        @Override
        protected <T> QueryScopeParam<T> createParam(final String name)
                throws QueryScope.MissingVariableException {
            if (!NameValidator.isValidQueryParameterName(name)) {
                throw new QueryScope.MissingVariableException("Name " + name + " is invalid");
            }
            // noinspection unchecked
            return new QueryScopeParam<>(name, (T) scriptSession.getVariable(name));
        }

        @Override
        public <T> T readParamValue(final String name) throws QueryScope.MissingVariableException {
            if (!NameValidator.isValidQueryParameterName(name)) {
                throw new QueryScope.MissingVariableException("Name " + name + " is invalid");
            }
            // noinspection unchecked
            return (T) scriptSession.getVariable(name);
        }

        @Override
        public <T> T readParamValue(final String name, final T defaultValue) {
            if (!NameValidator.isValidQueryParameterName(name)) {
                return defaultValue;
            }
            return scriptSession.getVariable(name, defaultValue);
        }

        @Override
        public <T> void putParam(final String name, final T value) {
            scriptSession.setVariable(NameValidator.validateQueryParameterName(name), value);
        }
    }

    public static class SynchronizedScriptSessionQueryScope extends UnsynchronizedScriptSessionQueryScope {
        public SynchronizedScriptSessionQueryScope(@NotNull final ScriptSession scriptSession) {
            super(scriptSession);
        }

        @Override
        public synchronized Set<String> getParamNames() {
            return super.getParamNames();
        }

        @Override
        public synchronized boolean hasParamName(String name) {
            return super.hasParamName(name);
        }

        @Override
        protected synchronized <T> QueryScopeParam<T> createParam(final String name)
                throws QueryScope.MissingVariableException {
            return super.createParam(name);
        }

        @Override
        public synchronized <T> T readParamValue(final String name) throws QueryScope.MissingVariableException {
            return super.readParamValue(name);
        }

        @Override
        public synchronized <T> T readParamValue(final String name, final T defaultValue) {
            return super.readParamValue(name, defaultValue);
        }

        @Override
        public synchronized <T> void putParam(final String name, final T value) {
            super.putParam(name, value);
        }
    }
}
