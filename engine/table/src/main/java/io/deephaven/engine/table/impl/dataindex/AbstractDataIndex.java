package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.DataIndexTransformer;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.RowSetColumnSourceWrapper;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.table.iterators.ColumnIterator;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class provides a data index for a table. The index is a table containing the key column(s) and the RowSets that
 * contain these values. DataIndexes may be loaded from storage or created in-memory using aggregations.
 */
public abstract class AbstractDataIndex implements DataIndex {
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
    static TObjectIntHashMap<Object> buildPositionMap(
            final Table indexTable,
            final String[] keyColumnNames,
            final boolean usePrev) {
        final RowSet rowSetToUse = usePrev ? indexTable.getRowSet().prev() : indexTable.getRowSet();

        int position = 0;
        // If we have only one key column, we will push values directly into the hashmap.
        if (keyColumnNames.length == 1) {
            TObjectIntHashMap<Object> result = new TObjectIntHashMap<>(indexTable.intSize(), 0.5f, -1);

            final ColumnSource<?> keyColumn = usePrev
                    ? indexTable.getColumnSource(keyColumnNames[0]).getPrevSource()
                    : indexTable.getColumnSource(keyColumnNames[0]);
            try (final CloseableIterator<Object> keyIterator = ChunkedColumnIterator.make(keyColumn, rowSetToUse)) {
                while (keyIterator.hasNext()) {
                    result.put(keyIterator.next(), position++);
                }
                return result;
            }
        } else {
            // Override the comparison and hashcode methods to handle arrays of keys.
            TObjectIntHashMap<Object> result = new TObjectIntHashMap<>(indexTable.intSize(), 0.5f, -1) {
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
                    .map(colName -> usePrev
                            ? indexTable.getColumnSource(colName).getPrevSource()
                            : indexTable.getColumnSource(colName))
                    .map(col -> ChunkedColumnIterator.make(col, rowSetToUse))
                    .toArray(ColumnIterator[]::new);

            while (keyIterators[0].hasNext()) {
                final Object[] complexKey = Arrays.stream(keyIterators).map(ColumnIterator::next).toArray();
                result.put(complexKey, position++);
            }

            SafeCloseableArray.close(keyIterators);

            return result;
        }
    }

    /**
     * Return the underlying table for this index. The resultant table should not be read directly; this method is
     * provided for snapshot controls to verify whether the ultimate parent table has ticked this cycle.
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

    /**
     * Return a copy of the table with the row set column replaced with a {@link RowSetColumnSourceWrapper wrapper}
     * column that converts getPrev() calls to prev() calls on the underlying row set.
     *
     * @param parent The table to copy
     * @param rowSetColumn The name of the row set column to wrap
     * @return the copied table
     */
    protected QueryTable wrappedRowSetTable(@NotNull final QueryTable parent, final String rowSetColumn) {
        // Clone the table but replace the row set column with the wrapper column.
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        parent.getColumnSourceMap().forEach((columnName, columnSource) -> {
            if (columnName.equals(rowSetColumn)) {
                columnSourceMap.put(columnName, RowSetColumnSourceWrapper.from(parent.getColumnSource(rowSetColumn)));
                return;
            }
            columnSourceMap.put(columnName, columnSource);
        });
        final QueryTable result = new QueryTable(parent.getRowSet(), columnSourceMap);
        if (parent.isRefreshing()) {
            result.setRefreshing(true);
            final BaseTable.ListenerImpl listener = new BaseTable.ListenerImpl("copy()",
                    parent, result);
            parent.addUpdateListener(listener, parent.getLastNotificationStep());
        }
        parent.copyAttributes(result, QueryTable.StandardOptions.COPY_ALL);
        return result;
    }
}

