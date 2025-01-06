//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import com.github.f4b6a3.uuid.UuidCreator;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.FileUtils;
import io.deephaven.configuration.CacheDir;
import io.deephaven.engine.context.*;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

import static io.deephaven.engine.table.Table.NON_DISPLAY_TABLE;

/**
 * This class exists to make all script sessions to be liveness artifacts, and provide a default implementation for
 * evaluateScript which handles liveness and diffs in a consistent way.
 */
public abstract class AbstractScriptSession<S extends AbstractScriptSession.Snapshot> extends LivenessArtifact
        implements ScriptSession {

    private static final Path CLASS_CACHE_LOCATION = CacheDir.get().resolve("script-session-classes");

    protected static Path newClassCacheLocation() {
        // TODO(deephaven-core#1713): Introduce instance-id concept
        final UUID scriptCacheId = UuidCreator.getRandomBased();
        final Path directory = CLASS_CACHE_LOCATION.resolve(UuidCreator.toString(scriptCacheId));
        createOrClearDirectory(directory);
        return directory;
    }

    public static void createScriptCache() {
        createOrClearDirectory(CLASS_CACHE_LOCATION);
    }

    private static void createOrClearDirectory(final Path directory) {
        if (Files.exists(directory)) {
            FileUtils.deleteRecursively(directory.toFile());
        }
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedDeephavenException(
                    "Failed to create class cache directory " + directory.toAbsolutePath(), e);
        }
    }

    private final ObjectTypeLookup objectTypeLookup;
    private final Listener changeListener;
    protected final File classCacheDirectory;
    private final ScriptSessionQueryScope queryScope;

    protected final ExecutionContext executionContext;

    private S lastSnapshot;

    protected AbstractScriptSession(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer,
            final ObjectTypeLookup objectTypeLookup,
            @Nullable final Listener changeListener) {
        this(updateGraph, operationInitializer, objectTypeLookup, changeListener, newClassCacheLocation().toFile(),
                Thread.currentThread().getContextClassLoader());
    }

    protected AbstractScriptSession(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer,
            final ObjectTypeLookup objectTypeLookup,
            @Nullable final Listener changeListener,
            @NotNull final File classCacheDirectory,
            @NotNull final ClassLoader parentClassLoader) {
        this.objectTypeLookup = objectTypeLookup;
        this.changeListener = changeListener;

        this.classCacheDirectory = classCacheDirectory;

        queryScope = new ScriptSessionQueryScope();
        final QueryCompiler compilerContext = QueryCompilerImpl.create(classCacheDirectory, parentClassLoader);

        executionContext = ExecutionContext.newBuilder()
                .markSystemic()
                .newQueryLibrary()
                .setQueryScope(queryScope)
                .setQueryCompiler(compilerContext)
                .setUpdateGraph(updateGraph)
                .setOperationInitializer(operationInitializer)
                .build();
    }

    @Override
    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    protected synchronized void publishInitial() {
        lastSnapshot = emptySnapshot();
        observeScopeChanges();
    }

    @Override
    public synchronized void observeScopeChanges() {
        final S beforeSnapshot = lastSnapshot;
        lastSnapshot = takeSnapshot();
        try (beforeSnapshot) {
            applyDiff(beforeSnapshot, lastSnapshot);
        }
    }

    protected interface Snapshot extends SafeCloseable {
    }

    protected abstract S emptySnapshot();

    protected abstract S takeSnapshot();

    protected abstract Changes createDiff(S from, S to, RuntimeException e);

    private void applyDiff(S from, S to) {
        if (changeListener != null) {
            final Changes diff = createDiff(from, to, null);
            changeListener.onScopeChanges(this, diff);
        }
    }

    @Override
    public synchronized final Changes evaluateScript(final String script, @Nullable final String scriptName) {
        // Observe scope changes and propagate to the listener before running the script, in case it is long-running
        observeScopeChanges();

        RuntimeException evaluateErr = null;
        final Changes diff;

        // retain any objects which are created in the executed code, we'll release them when the script session
        // closes
        try (final S initialSnapshot = takeSnapshot();
                final SafeCloseable ignored = LivenessScopeStack.open(queryScope, false)) {

            try {
                // Actually evaluate the script; use the enclosing auth context, since AbstractScriptSession's
                // ExecutionContext never has a non-null AuthContext
                executionContext.withAuthContext(ExecutionContext.getContext().getAuthContext())
                        .withQueryScope(queryScope)
                        .apply(() -> evaluate(script, scriptName));
            } catch (final RuntimeException err) {
                evaluateErr = err;
            }

            // Observe changes during this evaluation (potentially capturing external changes from other threads)
            observeScopeChanges();
            // Use the "last" snapshot created as a side effect of observeScopeChanges() as our "to"
            diff = createDiff(initialSnapshot, lastSnapshot, evaluateErr);
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
            return Optional.of("Table");
        }
        if (object instanceof HierarchicalTable) {
            return Optional.of("HierarchicalTable");
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

    @OverridingMethodsMustInvokeSuper
    @Override
    protected void destroy() {
        super.destroy();

        // Release a reference to the query scope, so if we're the last ones holding the bag, the scope and its contents
        // can go away
        queryScope.release();

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

    public QueryScope getQueryScope() {
        return queryScope;
    }

    /**
     * Retrieve a variable from the script session's bindings. Values may need to be unwrapped.
     *
     * @param name the name of the variable to retrieve
     * @return the variable value
     * @throws QueryScope.MissingVariableException if the variable does not exist
     */
    protected abstract <T> T getVariable(String name) throws QueryScope.MissingVariableException;

    /**
     * Retrieves all variable names present in the session's scope.
     *
     * @return a caller-owned mutable set of variable names
     */
    protected abstract Set<String> getVariableNames();

    /**
     * Check if the scope has the given variable name.
     *
     * @param name the variable name
     * @return True iff the scope has the given variable name
     */
    protected abstract boolean hasVariable(String name);

    /**
     * Inserts a value into the script's scope.
     *
     * @param name the variable name to set
     * @param value the new value of the variable
     * @return the old value for this name, if any. As with {@link #getVariable(String)}, may need to be unwrapped.
     */
    protected abstract Object setVariable(String name, @Nullable Object value);

    /**
     * Returns a mutable map with all known variables and their values.
     * <p>
     * Callers may want to pass in a valueMapper of {@link #unwrapObject(Object)} which would unwrap values before
     * filtering. The returned map is owned by the caller.
     *
     * @param valueMapper a function to map the values
     * @param filter a predicate to filter the map entries
     * @return a caller-owned mutable map with all known variables and their mapped values. As with
     *         {@link #getVariable(String)}, values may need to be unwrapped.
     * @param <T> the type of the mapped values
     */
    protected abstract <T> Map<String, T> getAllValues(
            @Nullable Function<Object, T> valueMapper, @NotNull QueryScope.ParamFilter<T> filter);

    // -----------------------------------------------------------------------------------------------------------------
    // ScriptSession-based QueryScope implementation, with no remote scope or object reflection support
    // -----------------------------------------------------------------------------------------------------------------

    public class ScriptSessionQueryScope extends LivenessScope implements QueryScope {
        /**
         * Internal workaround to support python calling pushScope.
         */
        public ScriptSession scriptSession() {
            return AbstractScriptSession.this;
        }

        @Override
        public Set<String> getParamNames() {
            return getVariableNames();
        }

        @Override
        public boolean hasParamName(String name) {
            return hasVariable(name);
        }

        @Override
        public <T> QueryScopeParam<T> createParam(final String name)
                throws QueryScope.MissingVariableException {
            return new QueryScopeParam<>(name, readParamValue(name));
        }

        @Override
        public <T> T readParamValue(final String name) throws QueryScope.MissingVariableException {
            return getVariable(name);
        }

        @Override
        public <T> T readParamValue(final String name, final T defaultValue) {
            try {
                return getVariable(name);
            } catch (MissingVariableException e) {
                return defaultValue;
            }
        }

        @Override
        public <T> void putParam(final String name, final T value) {
            if (value instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(value)) {
                manage((LivenessReferent) value);
            }

            Object oldValue = setVariable(name, value);

            Object unwrappedOldValue = unwrapObject(oldValue);

            if (unwrappedOldValue instanceof LivenessReferent
                    && DynamicNode.notDynamicOrIsRefreshing(unwrappedOldValue)) {
                unmanage((LivenessReferent) unwrappedOldValue);
            }
        }

        @Override
        public Map<String, Object> toMap(@NotNull final ParamFilter<Object> filter) {
            return AbstractScriptSession.this.getAllValues(null, filter);
        }

        @Override
        public <T> Map<String, T> toMap(
                @NotNull final Function<Object, T> valueMapper,
                @NotNull final ParamFilter<T> filter) {
            return AbstractScriptSession.this.getAllValues(valueMapper, filter);
        }

        @Override
        public Object unwrapObject(@Nullable Object object) {
            return AbstractScriptSession.this.unwrapObject(object);
        }
    }
}
