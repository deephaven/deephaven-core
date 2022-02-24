/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableMap;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.UnionColumnSource;
import io.deephaven.engine.table.impl.sources.UnionSourceManager;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.util.TableToolsMergeHelper;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.systemicmarking.SystemicObject;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.thread.NamingThreadFactory;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * A TableMap implementation backed by a Map.
 */
public class LocalTableMap extends TableMapImpl implements NotificationQueue.Dependency, SystemicObject {

    /** map to use for our backing store */
    private final Map<Object, Table> internalMap;

    /**
     * The expected definitions for the values in internalMap.
     */
    private final TableDefinition constituentDefinition;

    /**
     * A flattened version of this TableMap.
     */
    private TableMap cachedFlat;

    /**
     * If we've already flattened ourselves, there is no point in doing it again
     */
    private boolean isFlat;

    /**
     * Is this a systemically created TableMap?
     */
    private boolean isSystemic;

    /**
     * If true, we should use the global thread pool settings for transformation.
     */
    transient boolean useGlobalTransformationThreadPool = true;

    /**
     * If useGlobalTransformationThreadPool is false, how many threads should we use for our executorService?
     */
    transient int transformationThreads = 1;

    /**
     * Our executor service for transforming tables.
     */
    private transient ExecutorService executorService = null;

    /**
     * The TableMaps don't know how to create empty tables for themselves, if the TableMap needs to be pre-populated, it
     * must pass this callback into the constructor. When the populateKeys call on the TableMap is invoked, the populate
     * function is called for each of the new keys.
     */
    @FunctionalInterface
    public interface PopulateCallback {
        void populate(Object key);
    }

    private final PopulateCallback populateCallback;

    /**
     * If we have a dependency that merged results depend on, store it here.
     */
    private NotificationQueue.Dependency dependency;

    /**
     * Cache the last satisfied step, so that we do not need to query all our dependencies if we have already been
     * satisfied on a given cycle.
     */
    private long lastSatisfiedStep = -1;

    /**
     * Constructor to create an instance with a specific map, which may not be populated.
     *
     * @param internalMap the map to use for our backing store
     * @param populateCallback the callback that is invoked when {@link #populateKeys(Object...)} is called
     * @param constituentDefinition the definition of the constituent tables (optional, but by providing it, a TableMap
     *        with no constituents can be merged)
     */
    LocalTableMap(Map<Object, Table> internalMap, PopulateCallback populateCallback,
            TableDefinition constituentDefinition) {
        this.populateCallback = populateCallback;
        this.internalMap = internalMap;
        this.constituentDefinition = constituentDefinition;
        this.isSystemic = SystemicObjectTracker.isSystemicThread();
    }

    /**
     * Constructor to create an instance with an empty default map.
     *
     * @param populateCallback the callback that is invoked when {@link #populateKeys(Object...)} is called
     */
    public LocalTableMap(PopulateCallback populateCallback) {
        this(new LinkedHashMap<>(), populateCallback, null);
    }

    /**
     * Constructor to create an instance with an empty default map.
     *
     * @param populateCallback the callback that is invoked when {@link #populateKeys(Object...)} is called
     * @param constituentDefinition the definition of the constituent tables (optional, but by providing it, a TableMap
     *        with no constituents can be merged)
     */
    public LocalTableMap(PopulateCallback populateCallback, TableDefinition constituentDefinition) {
        this(new LinkedHashMap<>(), populateCallback, constituentDefinition);
    }

    public PopulateCallback getCallback() {
        return populateCallback;
    }

    @Override
    public boolean isSystemicObject() {
        return isSystemic;
    }

    @Override
    public void markSystemic() {
        isSystemic = true;
    }

    /**
     * Add a table to the map with the given key. Return the previous value, if any.
     *
     * @param key the key to add
     * @param table the value to add
     * @return the previous table for the given key
     */
    public Table put(Object key, Table table) {
        final Table result;

        result = putInternal(key, table);

        notifyKeyListeners(key);
        notifyListeners(key, table);

        return result;
    }

    @Nullable
    private synchronized Table putInternal(Object key, Table table) {
        if (constituentDefinition != null) {
            if (!constituentDefinition.equalsIgnoreOrder(table.getDefinition())) {
                throw new IllegalStateException(
                        "Put table does not match expected constituent definition: "
                                + constituentDefinition
                                        .getDifferenceDescription(table.getDefinition(), "existing", "new", "\n    "));
            }
        }

        if (table.isRefreshing()) {
            setRefreshing(true);
            // We need to keep our members around and updating as long as we are in use.
            manage(table);
        }

        final Table result = internalMap.put(key, table);

        if (result != null && result.isRefreshing()) {
            // Don't change the order of these operations.
            LivenessScopeStack.peek().manage(result);
            unmanage(result);
        }
        return result;
    }

    public Table computeIfAbsent(Object key, Function<Object, Table> tableFactory) {
        final Table result;
        synchronized (this) {
            final Table existing = get(key);
            if (existing != null) {
                return existing;
            }
            result = tableFactory.apply(key);
            putInternal(key, result);
        }
        notifyKeyListeners(key);
        notifyListeners(key, result);
        return result;
    }

    @Override
    public synchronized Table get(Object key) {
        final Table result = internalMap.get(key);
        if (result != null && result.isRefreshing()) {
            LivenessScopeStack.peek().manage(result);
        }
        return result;
    }

    @Override
    public Table getWithTransform(Object key, Function<Table, Table> transform) {
        Table result = get(key);
        if (result == null) {
            return null;
        }
        return transform.apply(result);
    }

    @Override
    public synchronized Object[] getKeySet() {
        return internalMap.keySet().toArray();
    }

    @Override
    public synchronized Collection<Table> values() {
        // TODO: Should we manage all values with current liveness scope?
        return internalMap.values();
    }

    @Override
    public synchronized int size() {
        return internalMap.size();
    }

    @Override
    public TableMap populateKeys(Object... keys) {
        if (populateCallback == null)
            throw new UnsupportedOperationException();

        for (final Object key : keys) {
            populateCallback.populate(key);
        }

        return this;
    }

    @SuppressWarnings("unused")
    public synchronized void removeKeys(Object... keys) {
        for (final Object key : keys) {
            final Table removed = internalMap.remove(key);
            if (removed != null && removed.isRefreshing()) {
                unmanage(removed);
            }
        }
    }

    public synchronized boolean containsKey(Object key) {
        return internalMap.containsKey(key);
    }

    public synchronized Collection<Map.Entry<Object, Table>> entrySet() {
        // TODO: Should we manage all entry values with current liveness scope? Do so on Map.Entry.getValue()?
        return internalMap.entrySet();
    }

    @Override
    public synchronized TableMap flatten() {
        if (isFlat) {
            if (isRefreshing()) {
                manageWithCurrentScope();
            }
            return this;
        }

        if (Liveness.verifyCachedObjectForReuse(cachedFlat)) {
            return cachedFlat;
        }

        final ComputedTableMap result = (ComputedTableMap) transformTables(Table::flatten);
        result.setFlat();

        return cachedFlat = result;
    }

    @Override
    public <R> R apply(Function<TableMap, R> function) {
        return function.apply(this);
    }

    @Override
    public TableMap transformTablesWithKey(BiFunction<Object, Table, Table> function) {
        final TableDefinition returnDefinition;
        if (constituentDefinition != null) {
            final Table emptyTable = new QueryTable(constituentDefinition,
                    RowSetFactory.empty().toTracking(),
                    NullValueColumnSource.createColumnSourceMap(constituentDefinition));
            returnDefinition = function.apply(SENTINEL_KEY, emptyTable).getDefinition();
        } else {
            returnDefinition = null;
        }
        return transformTablesWithKey(returnDefinition, function);
    }

    @Override
    public TableMap transformTablesWithKey(TableDefinition returnDefinition,
            BiFunction<Object, Table, Table> function) {
        final boolean shouldClear = QueryPerformanceRecorder.setCallsite();
        try {
            final ComputedTableMap result = new ComputedTableMap(this, returnDefinition);
            final ExecutorService executorService = getTransformationExecutorService();

            if (executorService != null) {
                final boolean doCheck = UpdateGraphProcessor.DEFAULT.getCheckTableOperations();
                final boolean hasUgp = UpdateGraphProcessor.DEFAULT.sharedLock().isHeldByCurrentThread()
                        || UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread();
                final Map<Object, Future<Table>> futures = new LinkedHashMap<>();
                for (final Map.Entry<Object, Table> entry : entrySet()) {
                    futures.put(entry.getKey(), executorService.submit(() -> {
                        if (hasUgp || !doCheck) {
                            final boolean oldCheck = UpdateGraphProcessor.DEFAULT.setCheckTableOperations(false);
                            try {
                                return function.apply(entry.getKey(), entry.getValue());
                            } finally {
                                UpdateGraphProcessor.DEFAULT.setCheckTableOperations(oldCheck);
                            }
                        } else {
                            return function.apply(entry.getKey(), entry.getValue());
                        }
                    }));
                }
                for (final Map.Entry<Object, Future<Table>> entry : futures.entrySet()) {
                    final Table table;
                    try {
                        table = entry.getValue().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new UncheckedTableException("Failed to transform table for " + entry.getKey(), e);
                    }
                    result.insertWithApply(entry.getKey(), table, (k, t) -> t);
                }
            } else {
                for (final Map.Entry<Object, Table> entry : entrySet()) {
                    result.insertWithApply(entry.getKey(), entry.getValue(), function);
                }
            }

            final LivenessListener listener = new LivenessListener() {
                @Override
                public void handleTableAdded(Object key, Table table) {
                    result.addNotification(() -> result.insertWithApply(key, table, function));
                }
            };
            addListener(listener);

            if (isRefreshing()) {
                // if we are refreshing, we want to add a parent reference, which turns the result refreshing; and
                // ensures reachability
                result.addParentReference(listener);
            } else {
                // if we are static, we only want to ensure reachability, but not turn the result refreshing; the
                // listener will fire for
                // populate calls, so the child map gets a value filled in properly
                result.setListenerReference(listener);
            }

            return result;
        } finally {
            if (shouldClear) {
                QueryPerformanceRecorder.clearCallsite();
            }
        }
    }

    private synchronized ExecutorService getTransformationExecutorService() {
        if (useGlobalTransformationThreadPool) {
            if (OperationInitializationThreadPool.NUM_THREADS > 1) {
                return OperationInitializationThreadPool.executorService;
            } else {
                return null;
            }
        }
        if (executorService != null) {
            return executorService;
        }
        if (transformationThreads <= 1) {
            return null;
        }

        final ThreadGroup threadGroup = new ThreadGroup("LocalTableMapTransform");
        final NamingThreadFactory threadFactory =
                new NamingThreadFactory(threadGroup, LocalTableMap.class, "transformExecutor", true);
        executorService = Executors.newFixedThreadPool(transformationThreads, threadFactory);

        return executorService;
    }

    @Override
    public TableMap transformTablesWithMap(TableMap other, BiFunction<Table, Table, Table> function) {
        final boolean shouldClear = QueryPerformanceRecorder.setCallsite();

        try {
            final NotificationQueue.Dependency otherDependency =
                    other instanceof NotificationQueue.Dependency ? (NotificationQueue.Dependency) other : null;
            final ComputedTableMap result = new ComputedTableMap(this, null);

            final ExecutorService executorService = getTransformationExecutorService();

            if (executorService != null) {
                final boolean doCheck = UpdateGraphProcessor.DEFAULT.setCheckTableOperations(true);
                UpdateGraphProcessor.DEFAULT.setCheckTableOperations(doCheck);
                final boolean hasUgp = UpdateGraphProcessor.DEFAULT.sharedLock().isHeldByCurrentThread()
                        || UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread();
                final Map<Object, Future<Table>> futures = new LinkedHashMap<>();


                for (final Map.Entry<Object, Table> entry : entrySet()) {
                    final Table otherTable = other.get(entry.getKey());
                    if (otherTable != null) {
                        futures.put(entry.getKey(), executorService.submit(() -> {
                            if (hasUgp || !doCheck) {
                                final boolean oldCheck = UpdateGraphProcessor.DEFAULT.setCheckTableOperations(false);
                                try {
                                    return function.apply(entry.getValue(), otherTable);
                                } finally {
                                    UpdateGraphProcessor.DEFAULT.setCheckTableOperations(oldCheck);
                                }
                            } else {
                                return function.apply(otherTable, entry.getValue());
                            }
                        }));
                    }
                }
                for (final Map.Entry<Object, Future<Table>> entry : futures.entrySet()) {
                    final Table table;
                    try {
                        table = entry.getValue().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException("Failed to transform table for " + entry.getKey(), e);
                    }
                    result.insertWithApply(entry.getKey(), table, (k, t) -> t);
                }
            } else {
                for (final Map.Entry<Object, Table> entry : entrySet()) {
                    final Table otherTable = other.get(entry.getKey());
                    if (otherTable != null) {
                        result.insertWithApply(entry.getKey(), entry.getValue(), otherTable, function::apply);
                    }
                }
            }

            final boolean otherRefreshing = DynamicNode.isDynamicAndIsRefreshing(other);
            if (isRefreshing()) {
                final Listener thisListener = new LivenessListener() {
                    @Override
                    public void handleTableAdded(Object key, Table ourTable) {
                        result.addNotification(() -> {
                            final Table otherTable = other.get(key);
                            if (otherTable != null) {
                                synchronized (result) {
                                    if (result.containsKey(key)) {
                                        return;
                                    }
                                    result.insertWithApply(key, ourTable, otherTable, function::apply);
                                }
                            }
                        }, otherDependency);
                    }
                };
                addListener(thisListener);
                result.addParentReference(thisListener);
            }

            if (otherRefreshing) {
                result.addParentReference(other); // Sets result refreshing if necessary, adds a hard ref from result to
                                                  // other, and liveness reference from result to other

                final Listener otherListener = new LivenessListener() {
                    @Override
                    public void handleTableAdded(Object key, Table otherTable) {
                        result.addNotification(() -> {
                            final Table ourTable = get(key);
                            if (ourTable != null) {
                                synchronized (result) {
                                    if (result.containsKey(key)) {
                                        return;
                                    }
                                    result.insertWithApply(key, ourTable, otherTable, function::apply);
                                }
                            }
                        }, otherDependency);
                    }
                };

                other.addListener(otherListener);
                result.addParentReference(otherListener);
            }

            return result;
        } finally {
            if (shouldClear) {
                QueryPerformanceRecorder.clearCallsite();
            }
        }
    }

    @Override
    public Table asTable(boolean strictKeys, boolean allowCoalesce, boolean sanityCheckJoins) {
        return TableMapProxyHandler.makeProxy(this, strictKeys, allowCoalesce, sanityCheckJoins);
    }

    public boolean isMergeable() {
        if (constituentDefinition != null) {
            return true;
        }
        synchronized (this) {
            return !internalMap.isEmpty();
        }
    }

    // todo: elevate this to TableMap, and make it mandatory?
    public Optional<TableDefinition> getConstituentDefinition() {
        return Optional.ofNullable(constituentDefinition);
    }

    public TableDefinition getConstituentDefinitionOrErr() {
        if (constituentDefinition != null) {
            return constituentDefinition;
        }
        synchronized (this) {
            if (!internalMap.isEmpty()) {
                return internalMap.values().iterator().next().getDefinition();
            }
        }
        throw new UnsupportedOperationException(
                "Can not convert TableMap with no constituents, or constituent definition, into a Table.");
    }

    @Override
    public synchronized Table merge() {
        final Table merged = TableToolsMergeHelper.mergeTableMap(this);

        final Collection<? extends ColumnSource> columnSources = merged.getColumnSources();
        if (columnSources.isEmpty()) {
            throw new IllegalArgumentException("Can not call merge when tables have no columns!");
        }

        final ColumnSource maybeUnionColumnSource = columnSources.iterator().next();
        if (!(maybeUnionColumnSource instanceof UnionColumnSource)) {
            throw new IllegalStateException("Expected column source to be a UnionColumnSource.");
        }

        final UnionSourceManager unionSourceManager =
                ((UnionColumnSource) (maybeUnionColumnSource)).getUnionSourceManager();
        unionSourceManager.setDisallowReinterpret(); // TODO: Skip this call if we can determine that our entry set is
                                                     // static.
        if (isRefreshing()) {
            unionSourceManager.noteUsingComponentsIsUnsafe();
            unionSourceManager.setRefreshing();
            merged.addParentReference(this);
            final LivenessListener listener = new LivenessListener() {
                @Override
                public void handleTableAdded(Object key, Table table) {
                    unionSourceManager.addTable((QueryTable) table.coalesce(), true);
                }
            };
            addListener(listener);
            merged.addParentReference(listener);
            if (getDependency() != null) {
                merged.addParentReference(getDependency());
            }
        }
        return merged;
    }

    public void setDependency(NotificationQueue.Dependency dependency) {
        this.dependency = dependency;
    }

    private NotificationQueue.Dependency getDependency() {
        return dependency;
    }

    @Override
    public synchronized boolean satisfied(final long step) {
        if (lastSatisfiedStep == step) {
            return true;
        }
        if (dependency != null && !dependency.satisfied(step)) {
            return false;
        }
        final boolean constituentsSatisfied = internalMap.values().stream().allMatch(x -> {
            if (x instanceof NotificationQueue.Dependency) {
                return ((NotificationQueue.Dependency) x).satisfied(step);
            }
            return true;
        });
        if (!constituentsSatisfied) {
            return false;
        }
        lastSatisfiedStep = step;
        return true;
    }

    private static class ComputedTableMap extends LocalTableMap {

        private final LocalTableMap parent;
        @ReferentialIntegrity
        private Listener listenerReference;

        private final AtomicInteger outstandingNotifications = new AtomicInteger(0);

        private ComputedTableMap(LocalTableMap parent, TableDefinition constituentDefinition) {
            super(null, constituentDefinition);
            this.parent = parent;
            if (parent.isRefreshing()) {
                setRefreshing(true);
                // NB: No need to addParentReference - we hold a strong reference to our parent already for
                // populateKeys.
                manage(parent);
            }
            this.useGlobalTransformationThreadPool = parent.useGlobalTransformationThreadPool;
            this.transformationThreads = parent.transformationThreads;
        }

        @Override
        public TableMap populateKeys(Object... keys) {
            parent.populateKeys(keys);
            return this;
        }

        @Override
        public Table put(Object key, Table table) {
            throw new UnsupportedOperationException();
        }

        private void insertWithApply(Object key, Table input, BiFunction<Object, Table, Table> operator) {
            super.put(key,
                    SystemicObjectTracker.executeSystemically(isSystemicObject(), () -> operator.apply(key, input)));
        }

        private void insertWithApply(Object key, Table ourInput, Table otherInput, BinaryOperator<Table> operator) {
            super.put(key, SystemicObjectTracker.executeSystemically(isSystemicObject(),
                    () -> operator.apply(ourInput, otherInput)));
        }

        private void setFlat() {
            super.isFlat = true;
        }

        void setListenerReference(Listener listenerReference) {
            this.listenerReference = listenerReference;
        }

        @Override
        public boolean satisfied(final long step) {
            // if we have any pending insertions or our parent is not yet satisfied, then we are not satisfied
            if (outstandingNotifications.get() > 0) {
                return false;
            }
            if (!parent.satisfied(step)) {
                return false;
            }
            return super.satisfied(step);
        }

        void addNotification(Runnable runnable) {
            addNotification(runnable, null);
        }

        void addNotification(Runnable runnable, NotificationQueue.Dependency other) {
            if (!UpdateGraphProcessor.DEFAULT.isRefreshThread()) {
                runnable.run();
                return;
            }
            outstandingNotifications.incrementAndGet();
            UpdateGraphProcessor.DEFAULT.addNotification(new AbstractNotification(false) {
                @Override
                public void run() {
                    try {
                        runnable.run();
                    } catch (Exception originalException) {
                        try {
                            if (SystemicObjectTracker.isSystemic(ComputedTableMap.this)) {
                                AsyncClientErrorNotifier.reportError(originalException);
                            }
                        } catch (IOException e) {
                            throw new UncheckedTableException("Exception in ComputedTableMap", originalException);
                        }
                    } finally {
                        outstandingNotifications.decrementAndGet();
                    }
                }

                @Override
                public boolean canExecute(final long step) {
                    return parent.satisfied(step) && (other == null || other.satisfied(step));
                }

                @Override
                public LogOutput append(LogOutput output) {
                    return output.append("ComputedTableMap Notification{").append(System.identityHashCode(this))
                            .append("}");
                }
            });
        }
    }

    private abstract class LivenessListener extends LivenessArtifact implements Listener {
        @Override
        abstract public void handleTableAdded(Object key, Table table);

        @Override
        protected void destroy() {
            super.destroy();
            removeListener(this);
        }
    }

    @Override
    public String toString() {
        return "LocalTableMap{" + internalMap + '}';
    }

    @Override
    protected void destroy() {
        if (executorService != null) {
            executorService.shutdown();
        }
        super.destroy();
    }

    /**
     * Returns whether this LocalTableMap is configured to use the global transformation thread pool.
     *
     * Derived TableMaps will inherit this setting (but use their own thread pool).
     *
     * @return true if transformTables and transformTablesWithMap will use the global thread pool; false if they will
     *         use a private thread pool
     */
    public boolean useGlobalTransformationThreadPool() {
        return useGlobalTransformationThreadPool;
    }

    /**
     * Sets whether this LocalTableMap is configured to use the global transformation thread pool.
     *
     * When set to true, the global thread pool configured in {@link OperationInitializationThreadPool} is used.
     *
     * When set to false, a thread pool for this particular TableMap is used (or no thread pool if transformation
     * threads is set to 1).
     */
    public synchronized void setUseGlobalTransformationThreadPool(boolean useGlobalTransformationThreadPool) {
        this.useGlobalTransformationThreadPool = useGlobalTransformationThreadPool;
        if (useGlobalTransformationThreadPool && executorService != null) {
            executorService.shutdown();
            executorService = null;
        }
    }

    /**
     * Returns the number of transformation threads that will be used (if this TableMap is not configured to use the
     * global thread pool). If this TableMap is configured to use the global thread pool, then this value is ignored.
     *
     * @return the number of threads that will be used for transformations
     */
    public int getTransformationThreads() {
        return transformationThreads;
    }

    /**
     * Set the number of transformation threads that should be used. Additionally, the global transformation thread pool
     * is disabled for this TableMap.
     *
     * Derived TableMaps will inherit this setting (but use their own thread pool).
     *
     * @param transformationThreads the number of threads that should be used for transformations
     */
    public synchronized void setTransformationThreads(int transformationThreads) {
        this.useGlobalTransformationThreadPool = false;
        this.transformationThreads = transformationThreads;
        if (executorService != null) {
            executorService.shutdown();
            executorService = null;
        }
    }
}
