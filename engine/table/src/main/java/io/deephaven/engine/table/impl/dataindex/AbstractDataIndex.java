package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.DataIndexTransformer;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.iterators.ColumnIterator;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * This class provides a data index for a table. The index is a table containing the key column(s) and the RowSets that
 * contain these values. DataIndexes may be loaded from storage or created in-memory using aggregations.
 */
public abstract class AbstractDataIndex implements DataIndex {
    protected static final int BIN_SEARCH_THRESHOLD = 1 << 20;
    public static final String INDEX_COL_NAME = "dh_row_set";

    @Override
    public DataIndex transform(@NotNull final DataIndexTransformer transformer) {
        return DerivedDataIndex.from(this, transformer);
    }

    /**
     * Build a map from the keys of the provided index table to positions in the table.
     *
     * @param indexTable the table to search
     * @param keyColumnNames the key columns to search
     * @return a map from keys to table positions
     */
    static TObjectIntHashMap<Object> buildPositionMap(final Table indexTable, final String[] keyColumnNames) {
        // If we have only one key column, we will push values directly into the hashmap.
        if (keyColumnNames.length == 1) {
            TObjectIntHashMap<Object> result = new TObjectIntHashMap<>(indexTable.intSize());
            try (final CloseableIterator<Object> keyIterator = indexTable.columnIterator(keyColumnNames[0])) {
                int position = 0;
                while (keyIterator.hasNext()) {
                    result.put(keyIterator.next(), position++);
                }
                return result;
            }
        } else {
            // Override the comparison and hashcode methods to handle arrays of keys.
            TObjectIntHashMap<Object> result = new TObjectIntHashMap<>(indexTable.intSize()) {
                @Override
                protected boolean equals(Object k1, Object k2) {
                    return Arrays.equals((Object[]) k1, (Object[]) k2);
                }

                @Override
                protected int hash(Object key) {
                    return Arrays.hashCode((Object[]) key);
                }
            };

            // Use Object[] as the keys for the map.
            ColumnIterator<?>[] keyIterators = Arrays.stream(keyColumnNames)
                    .map(indexTable::columnIterator).toArray(ColumnIterator[]::new);

            int position = 0;
            while (keyIterators[0].hasNext()) {
                final Object[] complexKey = Arrays.stream(keyIterators).map(ColumnIterator::next).toArray();
                result.put(complexKey, position++);
            }

            SafeCloseableArray.close(keyIterators);

            return result;
        }
    }

    /**
     * Build a lookup function that leverages binary search to successively narrow the matching rows until 1 or 0
     * remain. Requires that the table be sorted by the key columns.
     *
     * @param indexTable the table to search
     * @param keyColumnNames the key columns to search
     * @return
     */
    static @NotNull PositionLookup buildPositionBinSearchLookup(final Table indexTable, final String[] keyColumnNames) {
        final BinarySearcher[] keyVectors = Arrays.stream(keyColumnNames).map(colName -> BinarySearcher.from(
                indexTable.getColumnSource(colName),
                indexTable.getRowSet()))
                .toArray(BinarySearcher[]::new);

        final int tableSize = indexTable.getRowSet().intSize();

        return (Object o) -> {
            // o might be a single key or array of keys; force to an array
            final Object[] keys = o.getClass().isArray() ? (Object[]) o : new Object[] {o};

            if (keys.length != keyVectors.length) {
                throw new IllegalArgumentException("Expected " + keyVectors.length + " keys, got " + keys.length);
            }

            int start = 0;
            int end = tableSize;
            for (int ii = 0; ii < keys.length; ++ii) {
                // Iteratively reduce the key space by searching for each partial key.
                final BinarySearcher searcher = keyVectors[ii];
                final Object key = keys[ii];
                final int newStart = searcher.searchFirst(key, start, end);
                if (newStart < 0) {
                    // There was no match for this partial key, so there can't be a match for the whole array.
                    return -1;
                }
                start = newStart;
                final int newEnd = searcher.searchLast(key, start, end);
                if (newEnd < 0) {
                    // Only one match for this partial key.
                    end = start;
                    continue;
                }
                end = newEnd;
            }

            // The keyspace has been reduced to a single row, return the RowSet for that row.
            Assert.eq(start, "start", end, "end");
            return start;
        };
    }

    /**
     * Build a {@link PositionLookup lookup function} from the provided index table.
     *
     * @param indexTable the table to search
     * @param keyColumnNames the key columns to search
     * @return
     */
    static @NotNull PositionLookup buildPositionLookup(final Table indexTable, final String[] keyColumnNames) {
        return buildPositionMap(indexTable, keyColumnNames)::get;
    }

    /**
     * Return the underlying table for this index. The resultant table should not be read directly; this method is
     * provided for snapshot controls to verify whether the parent table has ticked this cycle.
     *
     * @return the underlying table supplying this index
     */
    public abstract Table baseIndexTable();

    /**
     * Whether this index is potentially usable. This will return {@code true} when there are no known issues for this
     * data index. This performs fast checks, such as verifying all locations have index table files but does not fully
     * guarantee that the index is complete and loadable.
     *
     * @return true if the index is potentially usable, false otherwise
     */
    public abstract boolean validate();
}

