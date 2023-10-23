/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.indexer;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.dataindex.TableBackedDataIndexImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

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

    @Override
    public void rowSetChanged() {
        // TODO: do we even need this call?
    }

    private static class DataIndexCache {

        /** The index at this level. */
        @Nullable
        DataIndex index;

        /** The sub-indexes below this level. */
        @Nullable
        WeakHashMap<ColumnSource<?>, DataIndexCache> dataIndexes;

        DataIndexCache(@Nullable final DataIndex index,
                @Nullable final WeakHashMap<ColumnSource<?>, DataIndexCache> dataIndexes) {
            this.index = index;
            this.dataIndexes = dataIndexes;
        }
    }


    public DataIndexer(@NotNull final TrackingRowSet rowSet) {
        this.rowSet = rowSet;
    }

    public boolean hasDataIndex(final ColumnSource<?>... keyColumns) {
        return hasDataIndex(Arrays.asList(keyColumns));
    }

    public boolean hasDataIndex(final List<ColumnSource<?>> keyColumns) {
        if (keyColumns.size() == 0) {
            return true;
        }

        if (dataIndexes == null) {
            return false;
        }

        return findIndex(dataIndexes, keyColumns) != null;
    }

    public boolean canMakeDataIndex(final ColumnSource<?>... keyColumns) {
        return canMakeDataIndex(Arrays.asList(keyColumns));
    }

    public boolean canMakeDataIndex(final List<ColumnSource<?>> keyColumns) {
        return true;
    }

    /**
     * Return a DataIndex for the given key columns, or null if no such index exists.
     *
     * @param keyColumns the column sources for which to retrieve or create a DataIndex
     * @return the DataIndex, or null
     */
    public DataIndex getDataIndex(final ColumnSource<?>... keyColumns) {
        return getDataIndex(null, keyColumns);
    }

    /**
     * Return a DataIndex for the given key columns, or null if no such index exists.
     *
     * @param sourceTable the table to index (if a new index is created by this operation)
     * @param keyColumns the column sources for which to retrieve or create a DataIndex
     * @return the DataIndex, or null
     */
    public DataIndex getDataIndex(final QueryTable sourceTable, final ColumnSource<?>... keyColumns) {
        final List<ColumnSource<?>> keys = Arrays.asList(keyColumns);
        // Return an index if one exists.
        DataIndex result = findIndex(dataIndexes, keys);
        if (result != null) {
            return result;
        }
        // Make a new index.
        return createDataIndex(sourceTable, keyColumns);
    }

    /**
     * Create a static DataIndex for the given key columns, returns null if no such index can be created.
     *
     * @param keyColumns the column sources to include in the index
     * @return the DataIndex, or null
     */
    public DataIndex createDataIndex(final ColumnSource<?>... keyColumns) {
        return createDataIndex(null, keyColumns);
    }

    /**
     * Create a refreshing DataIndex for the given table and key columns, returns null if no such index can be created.
     *
     * @param sourceTable the table to index
     * @param keyColumns the column sources to include in the index
     * @return the DataIndex, or null
     */
    public DataIndex createDataIndex(final QueryTable sourceTable, final ColumnSource<?>... keyColumns) {
        final List<ColumnSource<?>> keys = Arrays.asList(keyColumns);

        if (!canMakeDataIndex(keyColumns)) {
            // If we can't create an index, return null.
            return null;
        }

        final QueryTable tableToUse;
        if (sourceTable != null) {
            tableToUse = sourceTable;
        } else {
            // Create a dummy table for this index.
            int keyIndex = 0;
            final Map<String, ColumnSource<?>> cmp = new LinkedHashMap<>(keys.size());
            for (final ColumnSource<?> keySource : keys) {
                final String keyColumnName = "key" + keyIndex++;
                cmp.put(keyColumnName, keySource);
            }
            tableToUse = new QueryTable(rowSet, cmp);

            // Mark static so the created index table becomes static.
            tableToUse.setRefreshing(false);
        }

        // Create an index, add it to the correct map and return it.
        final DataIndex index = new TableBackedDataIndexImpl(tableToUse, keys);
        if (dataIndexes == null) {
            dataIndexes = new WeakHashMap<>();
        }
        addIndex(dataIndexes, keys, index);
        return index;
    }

    /**
     * Add a {@link DataIndex index} to the {@link DataIndexer}.
     *
     * @param index the index to add
     */
    public void addIndex(final DataIndex index) {
        final ColumnSource<?>[] keyColumns = index.keyColumnMap().keySet().toArray(ColumnSource<?>[]::new);
        final List<ColumnSource<?>> keys = Arrays.asList(keyColumns);

        addIndex(dataIndexes, keys, index);
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

            if (subCache.index != null) {
                resultList.add(subCache.index);
            }
            if (subCache.dataIndexes != null) {
                addIndexesToList(subCache.dataIndexes, resultList);
            }
        }
    }

    /** Remove the item at the specified index from a list. */
    private static <T> List<T> removeItem(List<T> items, int index) {
        List<T> result = new ArrayList<T>(items.size() - 1);
        result.addAll(items.subList(0, index));
        result.addAll(items.subList(index + 1, items.size() + 1));
        return result;
    }

    /** Check a specified map for an index with these column sources. */
    private static DataIndex findIndex(
            final WeakHashMap<ColumnSource<?>, DataIndexCache> map,
            final List<ColumnSource<?>> keyColumnSources) {
        if (map == null) {
            return null;
        }
        // Looking for a single key.
        if (keyColumnSources.size() == 1) {
            final DataIndexCache cache = map.get(keyColumnSources.get(0));
            return cache == null ? null : cache.index;
        }

        // Test every column source in the map for a match. This handles mis-ordered keys.
        for (int ii = 0; ii < keyColumnSources.size(); ++ii) {
            final ColumnSource<?> keyColumnSource = keyColumnSources.get(ii);
            final DataIndexCache cache = map.get(keyColumnSources.get(0));
            if (cache != null) {
                final List<ColumnSource<?>> sliced = removeItem(keyColumnSources, ii);

                // Recursively search for the remaining keys.
                final DataIndex index = findIndex(cache.dataIndexes, sliced);
                if (index != null) {
                    return index;
                }
            }
        }
        return null;
    }

    /** Add an index to the specified map. */
    private static void addIndex(
            final WeakHashMap<ColumnSource<?>, DataIndexCache> dataIndexes,
            final List<ColumnSource<?>> keyColumnSources,
            final DataIndex index) {
        // Only a single key remains.
        if (keyColumnSources.size() == 1) {
            final DataIndexCache cache = dataIndexes.get(keyColumnSources.get(0));
            if (cache != null) {
                // We should not be overwriting an existing index.
                Assert.eqNull(cache.index, "cache.index");
                cache.index = index;
            } else {
                dataIndexes.put(keyColumnSources.get(0), new DataIndexCache(index, null));
            }
            return;
        }

        DataIndexCache cache = dataIndexes.get(keyColumnSources.get(0));
        if (cache == null) {
            final WeakHashMap<ColumnSource<?>, DataIndexCache> subIndexes = new WeakHashMap<>();
            cache = new DataIndexCache(null, subIndexes);
            dataIndexes.put(keyColumnSources.get(0), cache);
        }
        final List<ColumnSource<?>> sliced = removeItem(keyColumnSources, 0);
        addIndex(cache.dataIndexes, sliced, index);
    }
}
