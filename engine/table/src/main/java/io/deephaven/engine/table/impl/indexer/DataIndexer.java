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
import io.deephaven.engine.table.impl.dataindex.TableBackedDataIndexImpl;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Indexer that provides single and multi-column clustering indexes for a table, linked to a {@link TrackingRowSet}.
 * 
 * @apiNote DataIndexers should not be used after the host {@link TrackingRowSet} has been {@link RowSet#close()
 *          closed}.
 */
public class DataIndexer implements TrackingRowSet.Indexer {
    public static DataIndexer of(TrackingRowSet rowSet) {
        return rowSet.indexer(DataIndexer::new);
    }

    private final TrackingRowSet rowSet;

    /**
     * The root of a pseudo-trie of index caches. This is a complicated structure but has the strong benefit of allowing
     * GC of data indexes belonging to GC'd column sources.
     */
    WeakHashMap<ColumnSource<?>, DataIndexCache> dataIndexes;

    private static class DataIndexCache {

        /** The index at this level. */
        @Nullable
        private volatile DataIndex thisLevelIndex;

        /** The sub-indexes below this level. */
        private final WeakHashMap<ColumnSource<?>, DataIndexCache> nextLevelCaches;

        DataIndexCache(@Nullable final DataIndex thisLevelIndex) {
            this.thisLevelIndex = thisLevelIndex;
            this.nextLevelCaches = new WeakHashMap<>();
        }
    }

    private DataIndexer(@NotNull final TrackingRowSet rowSet) {
        this.rowSet = rowSet;
        this.dataIndexes = new WeakHashMap<>();
    }

    public boolean hasDataIndex(final Table table, final String... keyColumnNames) {
        final DataIndex index = getDataIndex(table, keyColumnNames);
        if (index == null || !index.validate()) {
            // We do not have a usable index for these columns.
            return false;
        }
        return true;
    }

    public boolean hasDataIndex(final ColumnSource<?>... keyColumns) {
        return hasDataIndex(Arrays.asList(keyColumns));
    }

    public boolean hasDataIndex(final Collection<ColumnSource<?>> keyColumns) {
        if (keyColumns.isEmpty()) {
            return true;
        }

        if (dataIndexes == null) {
            return false;
        }

        final DataIndex dataIndex = findIndex(dataIndexes, keyColumns);
        // It's possible that a potentially indexed column might be corrupt or incomplete when we try to use it.
        // Test it now to make sure that we don't falsely claim to have a functional index.
        if (dataIndex != null && dataIndex.validate()) {
            return true;
        }
        // TODO: should we remove the index if it's corrupt? Could it repair itself, probably not.
        return false;
    }

    public boolean canMakeDataIndex(final Table table, final String... keyColumnNames) {
        return canMakeDataIndex(table, Arrays.asList(keyColumnNames));
    }

    public boolean canMakeDataIndex(final Table table, final Collection<String> keyColumnNames) {
        return true;
    }

    /**
     * Return a DataIndex for the given key columns, or null if no such index exists.
     *
     * @param keyColumns the column sources for which to retrieve a DataIndex
     * @return the DataIndex, or null if one does not exist
     */
    public DataIndex getDataIndex(final ColumnSource<?>... keyColumns) {
        return findIndex(dataIndexes, Arrays.asList(keyColumns));
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
        return findIndex(dataIndexes, keyColumns);
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
        if (dataIndexes == null) {
            dataIndexes = new WeakHashMap<>();
        }
        addIndex(dataIndexes, keyColumns, index, 0);
    }

    /**
     * Add a {@link DataIndex index} to the {@link DataIndexer}.
     *
     * @param index the index to add
     */
    public void addDataIndex(final DataIndex index) {
        final ColumnSource<?>[] keyColumns = index.keyColumnMap().keySet().toArray(ColumnSource<?>[]::new);
        final List<ColumnSource<?>> keys = Arrays.asList(keyColumns);

        addIndex(dataIndexes, keys, index, 0);
    }

    public List<DataIndex> dataIndexes() {
        final List<DataIndex> result = new ArrayList<>();

        if (dataIndexes != null) {
            addIndexesToList(dataIndexes, result);
        }

        return result;
    }

    private static void addIndexesToList(final WeakHashMap<ColumnSource<?>, DataIndexCache> dataIndexes,
            final List<DataIndex> resultList) {
        // Recurse through all the sub-indexes and collect DataIndexes.
        for (final Map.Entry<ColumnSource<?>, DataIndexCache> entry : dataIndexes.entrySet()) {
            final DataIndexCache subCache = entry.getValue();

            if (subCache.thisLevelIndex != null) {
                resultList.add(subCache.thisLevelIndex);
            }
            if (subCache.nextLevelCaches != null) {
                addIndexesToList(subCache.nextLevelCaches, resultList);
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
            synchronized (indexMap) {
                final DataIndexCache cache = indexMap.get(keyColumnSource);
                return cache == null ? null : cache.thisLevelIndex;
            }
        }

        // Test every column source in the map for a match. This handles mis-ordered keys.
        for (final ColumnSource<?> keySource : keyColumnSources) {
            final DataIndexCache cache = indexMap.get(keySource);
            if (cache != null) {
                // Create a new collection without the current key source.
                final Collection<ColumnSource<?>> sliced =
                        keyColumnSources.stream().filter(s -> s != keySource).collect(Collectors.toList());

                // Recursively search for the remaining key sources.
                final DataIndex index = findIndex(cache.nextLevelCaches, sliced);
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
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (indexMap) {
            final MutableBoolean created = new MutableBoolean(false);
            cache = indexMap.computeIfAbsent(
                    nextColumnSource,
                    ignored -> {
                        created.setTrue();
                        return new DataIndexCache(isLast ? index : null);
                    });
            if (!created.booleanValue()) {
                cache.thisLevelIndex = index; // Optimistically overwrite, it's volatile
            }
        }
        if (!isLast) {
            addIndex(cache.nextLevelCaches, keyColumnSources, index, nextColumnSourceIndex + 1);
        }
    }
}
