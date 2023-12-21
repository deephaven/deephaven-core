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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

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

    private final ReadWriteLock variableAccessLock = new ReentrantReadWriteLock();

    private final ObjectTypeLookup objectTypeLookup;
    private final Listener changeListener;
    private final File classCacheDirectory;
    private final QueryScope queryScope;

    protected final ExecutionContext executionContext;

    private S lastSnapshot;

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
        try (final S initialSnapshot = takeSnapshot();
                final SafeCloseable ignored = LivenessScopeStack.open(queryScope, false)) {

            try {
                // Actually evaluate the script; use the enclosing auth context, since AbstractScriptSession's
                // ExecutionContext never has a non-null AuthContext
                executionContext.withAuthContext(ExecutionContext.getContext().getAuthContext())
                        .apply(() -> {
                            // Take the write lock while running script code, so that readers can't look at variables
                            // again until the script has finished.
                            doLocked(variableAccessLock.writeLock(), () -> evaluate(script, scriptName));
                        });
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

    public QueryScope getQueryScope() {
        return queryScope;
    }

    /**
     * Retrieve a variable from the script session's bindings.
     *
     * @param name the variable to retrieve
     * @return the variable
     * @throws QueryScope.MissingVariableException if the variable does not exist
     */
    protected abstract <T> T getVariable(String name) throws QueryScope.MissingVariableException;

    /**
     * Retrieves all of the variable names present in the session's scope
     *
     * @return an immutable set of variable names
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

    private static void doLocked(@NotNull final Lock lock, @NotNull final Runnable runnable) {
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    private static <T> T doLocked(@NotNull final Lock lock, @NotNull final Supplier<T> supplier) {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ScriptSession-based QueryScope implementation, with no remote scope or object reflection support
    // -----------------------------------------------------------------------------------------------------------------

    public class ScriptSessionQueryScope extends QueryScope {
        /**
         * Internal workaround to support python calling pushScope.
         */
        public ScriptSession scriptSession() {
            return AbstractScriptSession.this;
        }

        @Override
        public Set<String> getParamNames() {
            final Set<String> result = new LinkedHashSet<>();
            Set<String> variables =
                    doLocked(variableAccessLock.readLock(), AbstractScriptSession.this::getVariableNames);
            for (final String name : variables) {
                if (NameValidator.isValidQueryParameterName(name)) {
                    result.add(name);
                }
            }
            return Collections.unmodifiableSet(result);
        }

        @Override
        public boolean hasParamName(String name) {
            return NameValidator.isValidQueryParameterName(name)
                    && doLocked(variableAccessLock.readLock(), () -> AbstractScriptSession.this.hasVariableName(name));
        }

        @Override
        protected <T> QueryScopeParam<T> createParam(final String name)
                throws QueryScope.MissingVariableException {
            if (!NameValidator.isValidQueryParameterName(name)) {
                throw new QueryScope.MissingVariableException("Name " + name + " is invalid");
            }
            return new QueryScopeParam<>(name, readParamValue(name));
        }

        @Override
        public <T> T readParamValue(final String name) throws QueryScope.MissingVariableException {
            if (!NameValidator.isValidQueryParameterName(name)) {
                throw new QueryScope.MissingVariableException("Name " + name + " is invalid");
            }
            variableAccessLock.readLock().lock();
            try {
                if (!hasVariableName(name)) {
                    throw new MissingVariableException("Missing variable " + name);
                }
                return AbstractScriptSession.this.getVariable(name);
            } finally {
                variableAccessLock.readLock().unlock();
            }
        }

        @Override
        public <T> T readParamValue(final String name, final T defaultValue) {
            if (!NameValidator.isValidQueryParameterName(name)) {
                return defaultValue;
            }
            return doLocked(variableAccessLock.readLock(), () -> AbstractScriptSession.this.getVariable(name));
        }

        @Override
        public <T> void putParam(final String name, final T value) {
            NameValidator.validateQueryParameterName(name);
            if (value instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(value)) {
                manage((LivenessReferent) value);
            }
            doLocked(variableAccessLock.writeLock(), () -> AbstractScriptSession.this.setVariable(name, value));
        }

        @Override
        public Object unwrapObject(Object object) {
            return AbstractScriptSession.this.unwrapObject(object);
        }
    }
}
