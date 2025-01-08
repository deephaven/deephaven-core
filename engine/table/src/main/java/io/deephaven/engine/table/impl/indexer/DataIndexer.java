//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.indexer;

import com.google.common.collect.Sets;
import io.deephaven.base.reference.HardSimpleReference;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.reference.WeakSimpleReference;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.dataindex.AbstractDataIndex;
import io.deephaven.engine.table.impl.dataindex.TableBackedDataIndex;
import io.deephaven.engine.table.impl.util.FieldUtils;
import io.deephaven.engine.updategraph.UpdateGraph;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Indexer that provides single and multi-column clustering indexes for a table, linked to a {@link TrackingRowSet}.
 * 
 * @apiNote DataIndexers should not be used after the host {@link TrackingRowSet} has been {@link RowSet#close()
 *          closed}.
 */
public class DataIndexer implements TrackingRowSet.Indexer {

    /**
     * DataIndexer lookup method. Use this call when you will query from the returned DataIndexer but not add new ones.
     *
     * @param rowSet The row set to index
     * @return The DataIndexer for {@code rowSet}, or null if none exists
     */
    public static DataIndexer existingOf(@NotNull final TrackingRowSet rowSet) {
        return rowSet.indexer();
    }

    /**
     * DataIndexer factory method. Use this call when you will add indexes to the returned DataIndexer.
     *
     * @param rowSet The row set to index
     * @return The DataIndexer for {@code rowSet}, created by this call if necessary
     */
    public static DataIndexer of(@NotNull final TrackingRowSet rowSet) {
        return rowSet.indexer((final TrackingRowSet indexedRowSet) -> new DataIndexer());
    }

    /**
     * The root of a pseudo-trie of index caches. This is a complicated structure but has the strong benefit of allowing
     * lookup without interfering with garbage collection of indexes or their indexed column sources.
     */
    private final DataIndexCache rootCache = new DataIndexCache();

    /**
     * Cache priority assignments, which allows us to provide an arbitrary total ordering over all indexed column
     * sources. This helps us to ensure that the "path" to any DataIndex is always the same, regardless of the order in
     * which column sources are provided.
     */
    private final Map<ColumnSource<?>, Integer> priorityAssignments = Collections.synchronizedMap(new WeakHashMap<>());

    /**
     * The next priority to assign to a column source. This is incremented each time a new column source is indexed.
     */
    private int nextPriority = 0;

    /**
     * Create a new DataIndexer.
     */
    private DataIndexer() {}

    private List<ColumnSource<?>> pathFor(@NotNull final Collection<ColumnSource<?>> keyColumns) {
        return keyColumns.stream()
                .sorted(Comparator.comparingInt(this::priorityOf))
                .collect(Collectors.toList());
    }

    private int priorityOf(@NotNull final ColumnSource<?> keyColumn) {
        return priorityAssignments.computeIfAbsent(keyColumn, ignoredMissingKeyColumn -> nextPriority++);
    }

    @NotNull
    private static Collection<ColumnSource<?>> getColumnSources(
            @NotNull final Table table,
            @NotNull final Collection<String> keyColumnNames) {
        return keyColumnNames.stream()
                .map(table::getColumnSource)
                .collect(Collectors.toList());
    }

    /**
     * Test whether {@code table} has a DataIndexer with a usable {@link DataIndex} for the given key columns. Note that
     * a result from this method is a snapshot of current state, and does not guarantee anything about future calls to
     * {@link #hasDataIndex}, {@link #getDataIndex}, or {@link #getOrCreateDataIndex(Table, String...)}.
     *
     * @param table The {@link Table} to check
     * @param keyColumnNames The key column names to check
     * @return Whether {@code table} has a DataIndexer with a {@link DataIndex} for the given key columns
     */
    public static boolean hasDataIndex(@NotNull final Table table, @NotNull final String... keyColumnNames) {
        return hasDataIndex(table, Arrays.asList(keyColumnNames));
    }

    /**
     * Test whether {@code table} has a DataIndexer with a usable {@link DataIndex} for the given key columns. Note that
     * a result from this method is a snapshot of current state, and does not guarantee anything about future calls to
     * {@link #hasDataIndex}, {@link #getDataIndex}, or {@link #getOrCreateDataIndex(Table, String...)}.
     *
     * @param table The {@link Table} to check
     * @param keyColumnNames The key column names to check
     * @return Whether {@code table} has a DataIndexer with a {@link DataIndex} for the given key columns
     */
    public static boolean hasDataIndex(@NotNull final Table table, @NotNull final Collection<String> keyColumnNames) {
        if (keyColumnNames.isEmpty()) {
            return false;
        }
        final Table tableToUse = table.coalesce();
        final DataIndexer indexer = DataIndexer.existingOf(tableToUse.getRowSet());
        if (indexer == null) {
            return false;
        }
        return indexer.hasDataIndex(getColumnSources(tableToUse, keyColumnNames));
    }

    /**
     * Test whether this DataIndexer has a usable {@link DataIndex} for the given key columns. Note that a result from
     * this method is a snapshot of current state, and does not guarantee anything about future calls to
     * {@link #hasDataIndex}, {@link #getDataIndex}, or {@link #getOrCreateDataIndex(Table, String...)}.
     *
     * @param keyColumns The key columns to check
     * @return Whether this DataIndexer has a {@link DataIndex} for the given key columns
     */
    public boolean hasDataIndex(@NotNull final ColumnSource<?>... keyColumns) {
        return hasDataIndex(Arrays.asList(keyColumns));
    }

    /**
     * Test whether this DataIndexer has a usable {@link DataIndex} for the given key columns. Note that a result from
     * this method is a snapshot of current state, and does not guarantee anything about future calls to
     * {@link #hasDataIndex}, {@link #getDataIndex}, or {@link #getOrCreateDataIndex(Table, String...)}.
     *
     * @param keyColumns The key columns to check
     * @return Whether this DataIndexer has a {@link DataIndex} for the given key columns
     */
    public boolean hasDataIndex(@NotNull final Collection<ColumnSource<?>> keyColumns) {
        if (keyColumns.isEmpty()) {
            return false;
        }
        return rootCache.contains(pathFor(keyColumns));
    }

    /**
     * If {@code table} has a DataIndexer, return a {@link DataIndex} for the given key columns, or {@code null} if no
     * such index exists, if the cached index is invalid, or if the {@link DataIndex#isRefreshing() refreshing} cached
     * index is no longer live.
     *
     * @param table The {@link Table} to check
     * @param keyColumnNames The key columns for which to retrieve a DataIndex
     * @return The {@link DataIndex}, or {@code null} if one does not exist
     */
    @Nullable
    public static DataIndex getDataIndex(@NotNull final Table table, final String... keyColumnNames) {
        return getDataIndex(table, Arrays.asList(keyColumnNames));
    }

    /**
     * If {@code table} has a DataIndexer, return a {@link DataIndex} for the given key columns, or {@code null} if no
     * such index exists, if the cached index is invalid, or if the {@link DataIndex#isRefreshing() refreshing} cached
     * index is no longer live.
     *
     * @param table The {@link Table} to check
     * @param keyColumnNames The key columns for which to retrieve a DataIndex
     * @return The {@link DataIndex}, or {@code null} if one does not exist
     */
    @Nullable
    public static DataIndex getDataIndex(@NotNull final Table table, final Collection<String> keyColumnNames) {
        if (keyColumnNames.isEmpty()) {
            return null;
        }
        final Table tableToUse = table.coalesce();
        final DataIndexer indexer = DataIndexer.existingOf(tableToUse.getRowSet());
        if (indexer == null) {
            return null;
        }
        return indexer.getDataIndex(getColumnSources(tableToUse, keyColumnNames));
    }

    /**
     * Return a {@link DataIndex} for the given key columns, or {@code null} if no such index exists, if the cached
     * index is invalid, or if the {@link DataIndex#isRefreshing() refreshing} cached index is no longer live.
     *
     * @param keyColumns The {@link ColumnSource column sources} for which to retrieve a {@link DataIndex}
     * @return The {@link DataIndex}, or {@code null} if one does not exist
     */
    public DataIndex getDataIndex(@NotNull final ColumnSource<?>... keyColumns) {
        return getDataIndex(Arrays.asList(keyColumns));
    }

    /**
     * Return a {@link DataIndex} for the given key columns, or {@code null} if no such index exists, if the cached
     * index is invalid, or if the {@link DataIndex#isRefreshing() refreshing} cached index is no longer live.
     *
     * @param keyColumns The {@link ColumnSource column sources} for which to retrieve a {@link DataIndex}
     * @return The {@link DataIndex}, or {@code null} if one does not exist
     */
    public DataIndex getDataIndex(@NotNull final Collection<ColumnSource<?>> keyColumns) {
        if (keyColumns.isEmpty()) {
            return null;
        }
        return rootCache.get(pathFor(keyColumns));
    }

    /**
     * Return a valid, live {@link DataIndex} for a strict subset of the given key columns, or {@code null} if no such
     * index exists. Will choose the DataIndex that results in the largest index table, following the assumption that
     * the largest index table will divide the source table into the most specific partitions.
     *
     * @param table The indexed {@link Table}
     * @param keyColumnNames The key column names to include
     *
     * @return The optimal partial {@link DataIndex}, or {@code null} if no such index exists
     */
    @Nullable
    public static DataIndex getOptimalPartialIndex(Table table, final String... keyColumnNames) {
        if (keyColumnNames.length == 0) {
            return null;
        }
        if (table.isRefreshing()) {
            table.getUpdateGraph().checkInitiateSerialTableOperation();
        }
        table = table.coalesce();
        final DataIndexer indexer = DataIndexer.existingOf(table.getRowSet());
        if (indexer == null) {
            return null;
        }
        final Set<ColumnSource<?>> keyColumns = Arrays.stream(keyColumnNames)
                .map(table::getColumnSource)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return LivenessScopeStack.computeEnclosed(() -> Sets.powerSet(keyColumns).stream()
                .filter(subset -> !subset.isEmpty() && subset.size() < keyColumns.size())
                .map(indexer::getDataIndex)
                .filter(Objects::nonNull)
                .max(Comparator.comparingLong(dataIndex -> dataIndex.table().size()))
                .orElse(null),
                table::isRefreshing, (final DataIndex result) -> result != null && result.isRefreshing());
    }

    /**
     * Create a {@link DataIndex} for {@code table} indexing {@code keyColumns}, if no valid, live data index already
     * exists for these inputs.
     *
     * @param table The {@link Table} to index
     * @param keyColumnNames The key column names to include
     * @return The existing or newly created {@link DataIndex}
     * @apiNote This method causes the returned {@link DataIndex} to be managed by the enclosing liveness manager.
     */
    public static DataIndex getOrCreateDataIndex(
            @NotNull final Table table,
            @NotNull final String... keyColumnNames) {
        return getOrCreateDataIndex(table, Arrays.asList(keyColumnNames));
    }

    /**
     * Create a {@link DataIndex} for {@code table} indexing {@code keyColumns}, if no valid, live data index already
     * exists for these inputs.
     *
     * @param table The {@link Table} to index
     * @param keyColumnNames The key column names to include
     * @return The existing or newly created {@link DataIndex}
     * @apiNote This method causes the returned {@link DataIndex} to be managed by the enclosing liveness manager.
     */
    public static DataIndex getOrCreateDataIndex(
            @NotNull final Table table,
            @NotNull final Collection<String> keyColumnNames) {
        if (keyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Cannot create a DataIndex without any key columns");
        }
        final QueryTable tableToUse = (QueryTable) table.coalesce();
        final DataIndexer dataIndexer = DataIndexer.of(tableToUse.getRowSet());
        return dataIndexer.rootCache.computeIfAbsent(dataIndexer.pathFor(getColumnSources(tableToUse, keyColumnNames)),
                () -> new TableBackedDataIndex(tableToUse, keyColumnNames.toArray(String[]::new)));
    }

    /**
     * Add a {@link DataIndex} to this DataIndexer.
     *
     * @param dataIndex The {@link DataIndex} to add
     * @throws IllegalStateException If a valid, live {@link DataIndex} already exists for the given key columns
     */
    public void addDataIndex(@NotNull final DataIndex dataIndex) {
        if (dataIndex.keyColumnNamesByIndexedColumn().isEmpty()) {
            throw new IllegalArgumentException("DataIndex must have at least one key column");
        }
        if (!rootCache.add(pathFor(dataIndex.keyColumnNamesByIndexedColumn().keySet()), dataIndex)) {
            throw new IllegalStateException(String.format("Attempted to add a duplicate index %s for key columns %s",
                    dataIndex, dataIndex.keyColumnNamesByIndexedColumn().keySet()));
        }
    }

    /**
     * @param ensureValidAndLive Whether to ensure that returned {@link DataIndex DataIndexes} are valid and live
     * @return All the {@link DataIndex DataIndexes} in this DataIndexer
     */
    public List<DataIndex> dataIndexes(final boolean ensureValidAndLive) {
        final List<DataIndex> result = new ArrayList<>();
        rootCache.getAll(result, ensureValidAndLive);
        return result;
    }

    /**
     * Test whether {@code dataIndex} is {@code null} or invalid for use. This test does not evaluate liveness.
     *
     * @param dataIndex The {@link DataIndex} to test
     * @return Whether {@code dataIndex} is {@code null} or invalid for use
     */
    private static boolean isInvalid(@Nullable final DataIndex dataIndex) {
        if (dataIndex == null) {
            return true;
        }
        if (dataIndex instanceof AbstractDataIndex && !((AbstractDataIndex) dataIndex).isValid()) {
            return true;
        }
        return dataIndex.isRefreshing() && dataIndex.table().isFailed();
    }

    /**
     * Test whether {@code dataIndex} is a non-{@code null} {@link DataIndex} that is currently valid and live for use.
     *
     * @param dataIndex The {@link DataIndex} to test
     * @return Whether {@code dataIndex} is valid and live for use
     */
    private static boolean isValidAndLive(@Nullable final DataIndex dataIndex) {
        if (isInvalid(dataIndex)) {
            return false;
        }
        boolean retained = false;
        try {
            return !dataIndex.isRefreshing() || (retained = dataIndex.tryRetainReference());
        } finally {
            if (retained) {
                dataIndex.dropReference();
            }
        }
    }

    /**
     * Similar to {@link io.deephaven.engine.liveness.Liveness#verifyCachedObjectForReuse(Object)}, but for
     * {@link DataIndex DataIndexes}.
     *
     * @param dataIndex The {@link DataIndex} to validate and manage if appropriate
     * @return {@code dataIndex}, if it can be used and is now safely managed by the enclosing
     *         {@link io.deephaven.engine.liveness.LivenessManager} if {@link DataIndex#isRefreshing() refreshing}, else
     *         {@code null}
     */
    private static DataIndex validateAndManageCachedDataIndex(@Nullable final DataIndex dataIndex) {
        if (isInvalid(dataIndex)) {
            return null;
        }
        if (!dataIndex.isRefreshing()) {
            return dataIndex;
        }
        /*
         * If we're looking up a data index from an update graph thread, this means we're almost certainly in a listener
         * and instantiating a new table operation. This could be a listener created directly by a user, or a
         * PartitionedTable transform (either from a PartitionedTable.Proxy or an explicit transform function). In any
         * case, if the data index isn't already satisfied, we would not want the operation being instantiated to wait
         * for it to become satisfied from an update graph thread. Consequently, it's best to pretend we don't have a
         * valid, live data index in this case. If a user or engine developer really wants to use a data index from an
         * update graph thread, they should have already looked it up and made their listener's Notification.canExecute
         * dependent on the data index table's satisfaction.
         */
        final UpdateGraph updateGraph = dataIndex.table().getUpdateGraph();
        if (updateGraph.currentThreadProcessesUpdates()
                && !dataIndex.table().satisfied(updateGraph.clock().currentStep())) {
            return null;
        }
        if (!LivenessScopeStack.peek().tryManage(dataIndex)) {
            return null;
        }
        return dataIndex;
    }

    /**
     * Interface for {@link DataIndex} implementations that may opt into strong reachability within the DataIndexer's
     * cache.
     */
    public interface RetainableDataIndex extends DataIndex {

        /**
         * @return Whether {@code this} should be strongly held (if {@link #addDataIndex(DataIndex) added}) to maintain
         *         reachability
         */
        boolean shouldRetain();

        /**
         * @return Whether {@code dataIndex} should be strongly held (if {@link #addDataIndex(DataIndex) added}) to
         *         maintain reachability
         */
        static boolean shouldRetain(@NotNull final DataIndex dataIndex) {
            return dataIndex instanceof RetainableDataIndex && ((RetainableDataIndex) dataIndex).shouldRetain();
        }
    }

    /**
     * Node structure for our multi-level cache of indexes.
     */
    private static class DataIndexCache {

        private static final Map<ColumnSource<?>, DataIndexCache> EMPTY_DESCENDANT_CACHES = Map.of();
        @SuppressWarnings("rawtypes")
        private static final AtomicReferenceFieldUpdater<DataIndexCache, Map> DESCENDANT_CACHES_UPDATER =
                AtomicReferenceFieldUpdater.newUpdater(DataIndexCache.class, Map.class, "descendantCaches");
        private static final SimpleReference<DataIndex> MISSING_INDEX_REFERENCE = new WeakSimpleReference<>(null);

        /** The sub-indexes below this level. */
        @SuppressWarnings("FieldMayBeFinal")
        private volatile Map<ColumnSource<?>, DataIndexCache> descendantCaches = EMPTY_DESCENDANT_CACHES;

        /** A reference to the index at this level. Note that there will never be an index at the "root" level. */
        private volatile SimpleReference<DataIndex> dataIndexReference = MISSING_INDEX_REFERENCE;

        private DataIndexCache() {}

        private Map<ColumnSource<?>, DataIndexCache> ensureDescendantCaches() {
            // noinspection unchecked
            return FieldUtils.ensureField(this, DESCENDANT_CACHES_UPDATER, EMPTY_DESCENDANT_CACHES,
                    () -> Collections.synchronizedMap(new WeakHashMap<>()));
        }

        private DataIndexCache ensureCache(@NotNull final ColumnSource<?> keyColumn) {
            return ensureDescendantCaches().computeIfAbsent(keyColumn, ignored -> new DataIndexCache());
        }

        /**
         * Recursively traverse the cache rooted at this node, calling {@code destinationCacheVisitor} if and when the
         * destination node is reached.
         *
         * @param path The column sources that define the path from the root to the destination
         * @param nextPathIndex The column source whose node we're trying to visit next when calling traverse
         * @param createMissingCaches Whether to create missing caches when traversing, or to return false if a cache is
         *        not found
         * @param destinationCacheVisitor The visitor to call when the destination node is reached
         * @return {@code false} if the destination node was not reached (only possible when
         *         {@code createMissingCaches==true}, else the result of {@code destinationCacheVisitor}
         */
        private boolean traverse(
                @NotNull List<ColumnSource<?>> path,
                final int nextPathIndex,
                final boolean createMissingCaches,
                @NotNull final Predicate<DataIndexCache> destinationCacheVisitor) {
            Require.inRange(nextPathIndex, "nextPathIndex", path.size(), "path.size()");
            final ColumnSource<?> nextColumnSource = path.get(nextPathIndex);
            final boolean nextIsDestination = nextPathIndex == path.size() - 1;
            DataIndexCache nextCache = descendantCaches.get(nextColumnSource);
            if (nextCache == null) {
                if (!createMissingCaches) {
                    return false;
                }
                nextCache = ensureCache(nextColumnSource);
            }
            if (nextIsDestination) {
                return destinationCacheVisitor.test(nextCache);
            }
            return nextCache.traverse(path, nextPathIndex + 1, createMissingCaches, destinationCacheVisitor);
        }

        /**
         * @param keyColumns The key columns to check
         * @return Whether this DataIndexCache contains a valid, live {@link DataIndex} for the given key columns
         */
        private boolean contains(@NotNull final List<ColumnSource<?>> keyColumns) {
            return traverse(keyColumns, 0, false, cache -> isValidAndLive(cache.dataIndexReference.get()));
        }

        /**
         * @param keyColumns The key columns to check
         * @return This DataIndexCache's valid, live {@link DataIndex} for the given key columns if it exists, else
         *         {@code null}
         * @apiNote This method causes the returned {@link DataIndex} to be managed by the enclosing liveness manager if
         *          it is {@link DataIndex#isRefreshing() refreshing}
         */
        @Nullable
        private DataIndex get(@NotNull final List<ColumnSource<?>> keyColumns) {
            final MutableObject<DataIndex> resultHolder = new MutableObject<>();
            traverse(keyColumns, 0, false, cache -> {
                resultHolder.setValue(validateAndManageCachedDataIndex(cache.dataIndexReference.get()));
                return true;
            });
            return resultHolder.getValue();
        }

        /**
         * Get all the {@link DataIndex DataIndexes} in this cache and its descendants.
         *
         * @param result The list to which to add the {@link DataIndex DataIndexes}
         * @param ensureValidAndLive Whether to first ensure that each {@link DataIndex} is valid and live for use
         */
        private void getAll(@NotNull final List<DataIndex> result, final boolean ensureValidAndLive) {
            final DataIndex localDataIndex = ensureValidAndLive
                    ? validateAndManageCachedDataIndex(dataIndexReference.get())
                    : dataIndexReference.get();
            if (localDataIndex != null) {
                result.add(localDataIndex);
            }
            descendantCaches.values().forEach(cache -> cache.getAll(result, ensureValidAndLive));
        }

        /**
         * @param keyColumns The key columns
         * @param dataIndex The {@link DataIndex} to add
         * @return Whether the {@code dataIndex} was added. {@code false} means a valid, live {@link DataIndex} for
         *         {@code keyColumns} was already present.
         * @apiNote No validation is done of the {@link DataIndex}. We intentionally defer until {@link #contains(List)
         *          contains}, {@link #get(List) get}, or {@link #computeIfAbsent(List, Supplier) computeIfAbsent}
         *          accesses it.
         */
        private boolean add(@NotNull final List<ColumnSource<?>> keyColumns, @NotNull final DataIndex dataIndex) {
            return traverse(keyColumns, 0, true, cache -> {
                if (!isValidAndLive(cache.dataIndexReference.get())) {
                    // noinspection SynchronizationOnLocalVariableOrMethodParameter
                    synchronized (cache) {
                        if (!isValidAndLive(cache.dataIndexReference.get())) {
                            cache.dataIndexReference = RetainableDataIndex.shouldRetain(dataIndex)
                                    ? new HardSimpleReference<>(dataIndex)
                                    : new WeakSimpleReference<>(dataIndex);
                            return true;
                        }
                    }
                }
                return false;
            });
        }

        /**
         * @param keyColumns The key columns
         * @param dataIndexFactory A {@link Supplier} for a new {@link DataIndex}, invoked if no valid, live
         *        {@link DataIndex} currently exists in the cache
         * @return The currently cached {@link DataIndex}
         * @apiNote If an existing {@link DataIndex} is returned, this method will test that it remains valid and will
         *          cause it to be managed by the enclosing liveness manager if it is {@link DataIndex#isRefreshing()
         *          refreshing}. If a new {@link DataIndex} is created, it is the caller's responsibility to ensure that
         *          it is valid and managed; this method defers validation as in {@link #add(List, DataIndex) add}.
         */
        private DataIndex computeIfAbsent(
                @NotNull final List<ColumnSource<?>> keyColumns,
                @NotNull final Supplier<DataIndex> dataIndexFactory) {
            final MutableObject<DataIndex> resultHolder = new MutableObject<>();
            traverse(keyColumns, 0, true, cache -> {
                DataIndex dataIndex = validateAndManageCachedDataIndex(cache.dataIndexReference.get());
                if (dataIndex == null) {
                    // noinspection SynchronizationOnLocalVariableOrMethodParameter
                    synchronized (cache) {
                        dataIndex = validateAndManageCachedDataIndex(cache.dataIndexReference.get());
                        if (dataIndex == null) {
                            // Don't manage the factory result with the enclosing liveness manager.
                            // It's the caller's responsibility to make sure we produce a valid data index that is
                            // managed by the appropriate scope for the caller's own use. Further validation is deferred
                            // as in add.
                            dataIndex = dataIndexFactory.get();
                            cache.dataIndexReference = new WeakSimpleReference<>(dataIndex);
                        }
                    }
                }
                resultHolder.setValue(dataIndex);
                return true;
            });
            return resultHolder.getValue();
        }
    }
}
