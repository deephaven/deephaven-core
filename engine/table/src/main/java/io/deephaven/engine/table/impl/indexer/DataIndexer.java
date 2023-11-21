/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.indexer;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.dataindex.BaseDataIndex;
import io.deephaven.engine.table.impl.dataindex.TableBackedDataIndexImpl;
import io.deephaven.engine.table.impl.util.FieldUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;

/**
 * Indexer that provides single and multi-column clustering indexes for a table, linked to a {@link TrackingRowSet}.
 * 
 * @apiNote DataIndexers should not be used after the host {@link TrackingRowSet} has been {@link RowSet#close()
 *          closed}.
 */
public class DataIndexer implements TrackingRowSet.Indexer {
    private static final AtomicReferenceFieldUpdater<DataIndexer, WeakHashMap> ROOT_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DataIndexer.class, WeakHashMap.class, "root");
    private static final WeakHashMap EMPTY_ROOT = new WeakHashMap<>();

    public static DataIndexer of(TrackingRowSet rowSet) {
        return rowSet.indexer(DataIndexer::new);
    }

    private final TrackingRowSet rowSet;

    /**
     * The root of a pseudo-trie of index caches. This is a complicated structure but has the strong benefit of allowing
     * GC of data indexes belonging to GC'd column sources.
     */
    private volatile WeakHashMap<ColumnSource<?>, DataIndexCache> root = EMPTY_ROOT;

    private static class DataIndexCache {

        /** The index at this level. */
        @Nullable
        private volatile DataIndex localIndex; // TODO-RWC: Weak reference

        /** The sub-indexes below this level. */
        private final WeakHashMap<ColumnSource<?>, DataIndexCache> descendantCaches;

        DataIndexCache(@Nullable final DataIndex localIndex) {
            this.localIndex = localIndex;
            this.descendantCaches = new WeakHashMap<>();
        }
    }

    private DataIndexer(@NotNull final TrackingRowSet rowSet) {
        this.rowSet = rowSet;
    }

    private WeakHashMap<ColumnSource<?>, DataIndexCache> ensureRoot() {
        // noinspection unchecked
        return FieldUtils.ensureField(this, ROOT_UPDATER, EMPTY_ROOT,
                WeakHashMap::new);
    }

    public boolean hasDataIndex(final Table table, final String... keyColumnNames) {
        final Collection<ColumnSource<?>> keyColumns = Arrays.stream(keyColumnNames)
                .map(table::getColumnSource).collect(Collectors.toList());
        return hasDataIndex(keyColumns);
    }

    public boolean hasDataIndex(final ColumnSource<?>... keyColumns) {
        return hasDataIndex(Arrays.asList(keyColumns));
    }

    public boolean hasDataIndex(final Collection<ColumnSource<?>> keyColumns) {
        final WeakHashMap<ColumnSource<?>, DataIndexCache> localRoot = root;
        if (keyColumns.isEmpty() || localRoot == EMPTY_ROOT) {
            return false;
        }

        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (localRoot) {
            final DataIndex dataIndex = findIndex(localRoot, keyColumns);
            return dataIndex != null && ((BaseDataIndex) dataIndex).validate();
        }
    }

    public boolean canMakeDataIndex(final Table table, final String... keyColumnNames) {
        return canMakeDataIndex(table, Arrays.asList(keyColumnNames));
    }

    public boolean canMakeDataIndex(final Table table, final Collection<String> keyColumnNames) {
        return keyColumnNames.size() > 0;
    }

    /**
     * Return a DataIndex for the given key columns, or null if no such index exists.
     *
     * @param keyColumns the column sources for which to retrieve a DataIndex
     * @return the DataIndex, or null if one does not exist
     */
    public DataIndex getDataIndex(final ColumnSource<?>... keyColumns) {
        final WeakHashMap<ColumnSource<?>, DataIndexCache> localRoot = root;
        if (localRoot == EMPTY_ROOT) {
            return null;
        }
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (localRoot) {
            return findIndex(localRoot, Arrays.asList(keyColumns));
        }
    }

    /**
     * Return a DataIndex for the given key columns, or null if no such index exists.
     *
     * @param sourceTable the table to index (if a new index is created by this operation)
     * @param keyColumnNames the column sources for which to retrieve a DataIndex
     * @return the DataIndex, or null if one does not exist
     */
    public DataIndex getDataIndex(final Table sourceTable, final String... keyColumnNames) {
        final Map<String, ? extends ColumnSource<?>> columnSourceMap = sourceTable.getColumnSourceMap();
        // Verify all the key columns belong to the source table.
        final Collection<String> missingKeys = Arrays.stream(keyColumnNames)
                .filter(key -> !columnSourceMap.containsKey(key)).collect(Collectors.toList());
        if (!missingKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "The following columns were not found in the provide table: " + missingKeys);
        }

        // Create a collection of the table key columns.
        final Collection<ColumnSource<?>> keyColumns = Arrays.stream(keyColumnNames)
                .map(columnSourceMap::get).collect(Collectors.toList());

        // Return an index if one exists.
        final WeakHashMap<ColumnSource<?>, DataIndexCache> localRoot = root;
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (localRoot) {
            return findIndex(ensureRoot(), keyColumns);
        }
    }

    /**
     * Create a refreshing DataIndex for the given table and key columns.
     *
     * @param sourceTable the table to index
     * @param keyColumnNames the column sources to include in the index
     */
    public void createDataIndex(final Table sourceTable, final String... keyColumnNames) {
        final List<String> keys = Arrays.asList(keyColumnNames);
        final List<ColumnSource<?>> keyColumns =
                keys.stream().map(sourceTable::getColumnSource).collect(Collectors.toList());

        if (hasDataIndex(keyColumns)) {
            // If we already have an index, throw an exception.
            throw new UnsupportedOperationException(
                    String.format("An index for %s already exists.", Arrays.toString(keyColumnNames)));
        }

        if (!canMakeDataIndex(sourceTable, keyColumnNames)) {
            // If we can't create an index, return an exception.
            throw new UnsupportedOperationException(
                    String.format("Cannot create index for %s", Arrays.toString(keyColumnNames)));
        }

        // Create an index, add it to the map and return it.
        QueryTable coalesced = (QueryTable) sourceTable.coalesce();
        final DataIndex index = new TableBackedDataIndexImpl(coalesced, keyColumnNames);

        final WeakHashMap<ColumnSource<?>, DataIndexCache> localRoot = ensureRoot();
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (localRoot) {
            addIndex(ensureRoot(), keyColumns, index, 0);
        }
    }

    /**
     * Add a {@link DataIndex index} to the {@link DataIndexer}.
     *
     * @param index the index to add
     */
    public void addDataIndex(final DataIndex index) {
        // TODO-RWC/LAB: We need to prevent "snapshot" derived data indexes from being added to the data indexer, here
        // and in create.
        final ColumnSource<?>[] keyColumns = index.keyColumnMap().keySet().toArray(ColumnSource<?>[]::new);
        final List<ColumnSource<?>> keys = Arrays.asList(keyColumns);

        final WeakHashMap<ColumnSource<?>, DataIndexCache> localRoot = ensureRoot();
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (localRoot) {
            addIndex(ensureRoot(), keys, index, 0);
        }
    }

    public List<DataIndex> dataIndexes() {
        final List<DataIndex> result = new ArrayList<>();

        if (root != null) {
            addIndexesToList(root, result);
        }

        return result;
    }

    private static void addIndexesToList(final WeakHashMap<ColumnSource<?>, DataIndexCache> dataIndexes,
            final List<DataIndex> resultList) {
        // Recurse through all the sub-indexes and collect DataIndexes.
        for (final Map.Entry<ColumnSource<?>, DataIndexCache> entry : dataIndexes.entrySet()) {
            final DataIndexCache subCache = entry.getValue();

            if (subCache.localIndex != null) {
                resultList.add(subCache.localIndex);
            }
            if (subCache.descendantCaches != null) {
                addIndexesToList(subCache.descendantCaches, resultList);
            }
        }
    }

    /** Check a specified map for an index with these column sources. */
    private static DataIndex findIndex(
            final WeakHashMap<ColumnSource<?>, DataIndexCache> indexMap,
            final Collection<ColumnSource<?>> keyColumnSources) {
        // Looking for a single key.
        if (keyColumnSources.size() == 1) {
            final ColumnSource<?> keyColumnSource = keyColumnSources.iterator().next();
            final DataIndexCache cache = indexMap.get(keyColumnSource);
            return cache == null ? null : cache.localIndex;
        }

        // Test every column source in the map for a match. This handles mis-ordered keys.
        for (final ColumnSource<?> keySource : keyColumnSources) {
            final DataIndexCache cache = indexMap.get(keySource);
            if (cache != null) {
                // Create a new collection without the current key source.
                final Collection<ColumnSource<?>> sliced =
                        keyColumnSources.stream().filter(s -> s != keySource).collect(Collectors.toList());

                // Recursively search for the remaining key sources.
                final DataIndex index = findIndex(cache.descendantCaches, sliced);
                if (index != null) {
                    return index;
                }
            }
        }
        return null;
    }

    /** Add an index to the specified map. */
    private static void addIndex(
            final WeakHashMap<ColumnSource<?>, DataIndexCache> indexMap,
            final List<ColumnSource<?>> keyColumnSources,
            final DataIndex index,
            final int nextColumnSourceIndex) {
        final ColumnSource<?> nextColumnSource = keyColumnSources.get(nextColumnSourceIndex);
        final boolean isLast = nextColumnSourceIndex == keyColumnSources.size() - 1;
        final DataIndexCache cache;
        final MutableBoolean created = new MutableBoolean(false);
        cache = indexMap.computeIfAbsent(
                nextColumnSource,
                ignored -> {
                    created.setTrue();
                    return new DataIndexCache(isLast ? index : null);
                });
        if (isLast && !created.booleanValue()) {
            cache.localIndex = index;
        }
        if (!isLast) {
            addIndex(cache.descendantCaches, keyColumnSources, index, nextColumnSourceIndex + 1);
        }
    }
}
