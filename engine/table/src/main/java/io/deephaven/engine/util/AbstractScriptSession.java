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
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.deephaven.engine.table.Table.NON_DISPLAY_TABLE;

/**
 * This class exists to make all script sessions to be liveness artifacts, and provide a default implementation for
 * evaluateScript which handles liveness and diffs in a consistent way.
 */
public abstract class AbstractScriptSession<S extends AbstractScriptSession.Snapshot> extends LivenessArtifact
        implements ScriptSession {

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

    private final ReadWriteLock variableAccessLock = new ReentrantReadWriteLock();

    private S lastSnapshot;
    private final QueryScope queryScope;

    protected AbstractScriptSession(
            UpdateGraph updateGraph,
            final ThreadInitializationFactory threadInitializationFactory,
            ObjectTypeLookup objectTypeLookup,
            @Nullable Listener changeListener) {
        this.objectTypeLookup = objectTypeLookup;
        this.changeListener = changeListener;

        // TODO(deephaven-core#1713): Introduce instance-id concept
        final UUID scriptCacheId = UuidCreator.getRandomBased();
        classCacheDirectory = CLASS_CACHE_LOCATION.resolve(UuidCreator.toString(scriptCacheId)).toFile();
        createOrClearDirectory(classCacheDirectory);

        queryScope = new ScriptSessionQueryScope();
        manage(queryScope);
        final QueryCompiler compilerContext =
                QueryCompiler.create(classCacheDirectory, Thread.currentThread().getContextClassLoader());

        executionContext = ExecutionContext.newBuilder()
                .markSystemic()
                .newQueryLibrary()
                .setQueryScope(queryScope)
                .setQueryCompiler(compilerContext)
                .setUpdateGraph(updateGraph)
                .setOperationInitializer(new OperationInitializationThreadPool(threadInitializationFactory))
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
        variableAccessLock.writeLock().lock();
        try (final S initialSnapshot = takeSnapshot();
                final SafeCloseable ignored = LivenessScopeStack.open(queryScope, false)) {

            try {
                // Actually evaluate the script; use the enclosing auth context, since AbstractScriptSession's
                // ExecutionContext never has a non-null AuthContext
                executionContext.withAuthContext(ExecutionContext.getContext().getAuthContext())
                        .apply(() -> evaluate(script, scriptName));
            } catch (final RuntimeException err) {
                evaluateErr = err;
            }

            // Observe changes during this evaluation (potentially capturing external changes from other threads)
            observeScopeChanges();
            // Use the "last" snapshot created as a side effect of observeScopeChanges() as our "to"
            diff = createDiff(initialSnapshot, lastSnapshot, evaluateErr);
        } finally {
            variableAccessLock.writeLock().unlock();
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
     * @return a query scope that wraps
     */
    public QueryScope getQueryScope() {
        return queryScope;
    }

    private Class<?> getVariableType(final String var) {
        final Object result = getVariable(var, null);
        if (result == null) {
            return null;
        } else if (result instanceof Table) {
            return Table.class;
        } else {
            return result.getClass();
        }
    }


    public TableDefinition getTableDefinition(final String var) {
        Object o = getVariable(var, null);
        return o instanceof Table ? ((Table) o).getDefinition() : null;
    }


    /**
     * Retrieve a variable from the script session's bindings.
     * <p/>
     * Please use {@link ScriptSession#getVariable(String, Object)} if you expect the variable may not exist.
     *
     * @param name the variable to retrieve
     * @return the variable
     * @throws QueryScope.MissingVariableException if the variable does not exist
     */
    @NotNull
    protected abstract Object getVariable(String name) throws QueryScope.MissingVariableException;

    /**
     * Retrieve a variable from the script session's bindings. If the variable is not present, return defaultValue.
     * <p>
     * If the variable is present, but is not of type (T), a ClassCastException may result.
     *
     * @param name the variable to retrieve
     * @param defaultValue the value to use when no value is present in the session's scope
     * @param <T> the type of the variable
     * @return the value of the variable, or defaultValue if not present
     */
    protected abstract <T> T getVariable(String name, T defaultValue);

    /**
     * Retrieves all of the variables present in the session's scope (e.g., Groovy binding, Python globals()).
     *
     * @return an unmodifiable map with variable names as the keys, and the Objects as the result
     */
    protected abstract Map<String, Object> getVariables();

    /**
     * Retrieves all of the variable names present in the session's scope
     *
     * @return an unmodifiable set of variable names
     */
    protected abstract Set<String> getVariableNames();

    /**
     * Check if the scope has the given variable name
     *
     * @param name the variable name
     * @return True iff the scope has the given variable name
     */
    protected abstract boolean hasVariableName(String name);

    /**
     * Inserts a value into the script's scope.
     *
     * @param name the variable name to set
     * @param value the new value of the variable
     */
    protected abstract void setVariable(String name, @Nullable Object value);

    @Override
    public VariableProvider getVariableProvider() {
        return new VariableProvider() {
            @Override
            public Set<String> getVariableNames() {
                variableAccessLock.readLock().lock();
                try {
                    return AbstractScriptSession.this.getVariableNames();
                } finally {
                    variableAccessLock.readLock().unlock();
                }
            }

            @Override
            public Class<?> getVariableType(String var) {
                variableAccessLock.readLock().lock();
                try {
                    return AbstractScriptSession.this.getVariableType(var);
                } finally {
                    variableAccessLock.readLock().unlock();
                }
            }

            @Override
            public <T> T getVariable(String var, T defaultValue) {
                variableAccessLock.readLock().lock();
                try {
                    return AbstractScriptSession.this.getVariable(var, defaultValue);
                } finally {
                    variableAccessLock.readLock().unlock();
                }
            }

            @Override
            public TableDefinition getTableDefinition(String var) {
                variableAccessLock.readLock().lock();
                try {
                    return AbstractScriptSession.this.getTableDefinition(var);
                } finally {
                    variableAccessLock.readLock().unlock();
                }
            }

            @Override
            public boolean hasVariableName(String name) {
                variableAccessLock.readLock().lock();
                try {
                    return AbstractScriptSession.this.hasVariableName(name);
                } finally {
                    variableAccessLock.readLock().unlock();
                }
            }

            @Override
            public <T> void setVariable(String name, T value) {
                variableAccessLock.writeLock().lock();
                try {
                    AbstractScriptSession.this.setVariable(name, value);
                } finally {
                    variableAccessLock.writeLock().unlock();
                }
            }
        };
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ScriptSession-based QueryScope implementation, with no remote scope or object reflection support
    // -----------------------------------------------------------------------------------------------------------------

    public class ScriptSessionQueryScope extends QueryScope {
        @Override
        public Set<String> getParamNames() {
            final Set<String> result = new LinkedHashSet<>();
            for (final String name : getVariableProvider().getVariableNames()) {
                if (NameValidator.isValidQueryParameterName(name)) {
                    result.add(name);
                }
            }
            return result;
        }

        @Override
        public boolean hasParamName(String name) {
            return NameValidator.isValidQueryParameterName(name) && getVariableProvider().hasVariableName(name);
        }

        @Override
        protected <T> QueryScopeParam<T> createParam(final String name)
                throws QueryScope.MissingVariableException {
            if (!NameValidator.isValidQueryParameterName(name)) {
                throw new QueryScope.MissingVariableException("Name " + name + " is invalid");
            }
            // noinspection unchecked
            return new QueryScopeParam<>(name, (T) getVariableProvider().getVariable(name, null));
        }

        @Override
        public <T> T readParamValue(final String name) throws QueryScope.MissingVariableException {
            if (!NameValidator.isValidQueryParameterName(name)) {
                throw new QueryScope.MissingVariableException("Name " + name + " is invalid");
            }
            // noinspection unchecked
            return (T) getVariableProvider().getVariable(name, null);
        }

        @Override
        public <T> T readParamValue(final String name, final T defaultValue) {
            if (!NameValidator.isValidQueryParameterName(name)) {
                return defaultValue;
            }
            return getVariableProvider().getVariable(name, defaultValue);
        }

        @Override
        public <T> void putParam(final String name, final T value) {
            if (value instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(value)) {
                manage((LivenessReferent) value);
            }
            getVariableProvider().setVariable(NameValidator.validateQueryParameterName(name), value);
        }

        @Override
        public Object unwrapObject(Object object) {
            return AbstractScriptSession.this.unwrapObject(object);
        }
    }
}
