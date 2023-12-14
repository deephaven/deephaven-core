package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.OperationSnapshotControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.RowSetColumnSourceWrapper;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.table.iterators.ColumnIterator;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * This class provides a data index for a table. The index is itself a table with columns corresponding to the indexed
 * key column(s) and a column of RowSets that contain the key values.
 */
public abstract class BaseDataIndex extends LivenessArtifact implements DataIndex {

    static final String INDEX_COL_NAME = "dh_row_set";

    @Override
    public BasicDataIndex transform(@NotNull final DataIndexTransformer transformer) {
        return TransformedDataIndex.from(this, transformer);
    }

    @Override
    public DataIndex remapKeyColumns(final @NotNull Map<ColumnSource<?>, ColumnSource<?>> oldToNewColumnMap) {
        return new RemappedDataIndex(this, oldToNewColumnMap);
    }

    /**
     * Whether this index is potentially usable. This will return {@code true} when there are no known issues for this
     * data index. This performs fast checks, such as verifying all locations have index table files, but does not fully
     * guarantee that the index is complete and loadable.
     *
     * @return true if the index is potentially usable, false otherwise
     */
    public abstract boolean validate();

    /**
     * Return a copy of {@code parent} with the row set column replaced with a {@link RowSetColumnSourceWrapper wrapper}
     * column that adds {@link TrackingRowSet#prev() prev} calls on access to previous values.
     *
     * @param parent The table to copy
     * @param rowSetColumn The name of the row set column to wrap
     * @return The copied table
     */
    protected static QueryTable indexTableWrapper(
            @NotNull final QueryTable parent,
            @NotNull final String rowSetColumn) {
        return indexTableWrapper(parent, rowSetColumn, rowSetColumn);
    }

    /**
     * Return a copy of {@code parent} with the row set column replaced with a {@link RowSetColumnSourceWrapper wrapper}
     * column that adds {@link TrackingRowSet#prev() prev} calls on access to previous values.
     *
     * @param parent The table to copy
     * @param rowSetColumn The name of the row set column to wrap
     * @param renamedRowSetColumn The name of the row set column in the output table
     * @return The copied table
     */
    protected static QueryTable indexTableWrapper(
            @NotNull final QueryTable parent,
            @NotNull final String rowSetColumn,
            @NotNull final String renamedRowSetColumn) {
        // TODO-RWC/LAB: Use new assertions to assert that parent has a RowSet ColumnSource of name rowSetColumn.
        final UpdateGraph updateGraph = parent.getUpdateGraph();
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            return QueryPerformanceRecorder.withNugget("wrapRowSetColumn()", parent.sizeForInstrumentation(), () -> {
                final Mutable<QueryTable> result = new MutableObject<>();
                final LinkedHashMap<String, ColumnSource<?>> resultColumnSourceMap =
                        new LinkedHashMap<>(parent.numColumns());
                parent.getColumnSourceMap().forEach((columnName, columnSource) -> {
                    if (columnName.equals(rowSetColumn)) {
                        resultColumnSourceMap.put(
                                renamedRowSetColumn,
                                RowSetColumnSourceWrapper.from(parent.getColumnSource(rowSetColumn)));
                    } else {
                        // Convert the key columns to primitive column sources.
                        resultColumnSourceMap.put(columnName, ReinterpretUtils.maybeConvertToPrimitive(columnSource));
                    }
                });
                final OperationSnapshotControl snapshotControl =
                        parent.createSnapshotControlIfRefreshing(OperationSnapshotControl::new);
                QueryTable.initializeWithSnapshot("wrapRowSetColumn", snapshotControl, (usePrev, beforeClockValue) -> {
                    final QueryTable resultTable = new QueryTable(TableDefinition.inferFrom(resultColumnSourceMap),
                            parent.getRowSet(), resultColumnSourceMap, null, parent.getAttributes());
                    parent.propagateFlatness(resultTable);
                    if (snapshotControl != null) {
                        final BaseTable.ListenerImpl listener =
                                new BaseTable.ListenerImpl("wrapRowSetColumn()", parent, resultTable);
                        snapshotControl.setListenerAndResult(listener, resultTable);
                    }

                    result.setValue(resultTable);
                    return true;
                });

                return result.getValue();
            });
        }
    }

    /**
     * Build a map from the lookup keys of the provided index table to row keys in the table.
     *
     * @param indexTable the table to search
     * @param keyColumnNames the key columns to search
     * @return a map from keys to table positions
     */
    public static TObjectLongHashMap<Object> buildKeyMap(
            final Table indexTable,
            final String[] keyColumnNames) {
        // TODO-RWC: Come back to this, since we might not want to keep it.
        final RowSet rowSetToUse = indexTable.getRowSet();

        // If we have only one key column, we will push values directly into the hashmap.
        if (keyColumnNames.length == 1) {
            TObjectLongHashMap<Object> result = new TObjectLongHashMap<>(indexTable.intSize(), 0.5f, NULL_ROW_KEY);

            final ColumnSource<?> keyColumn = indexTable.getColumnSource(keyColumnNames[0]);
            try (final CloseableIterator<Object> keyIterator = ChunkedColumnIterator.make(keyColumn, rowSetToUse);
                    final RowSet.Iterator rsIterator = rowSetToUse.iterator()) {
                while (keyIterator.hasNext()) {
                    result.put(keyIterator.next(), rsIterator.next());
                }
                return result;
            }
        } else {
            // Override the comparison and hashcode methods to handle arrays of keys.
            TObjectLongHashMap<Object> result = new TObjectLongHashMap<>(indexTable.intSize(), 0.5f, NULL_ROW_KEY) {
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
                    .map(indexTable::getColumnSource)
                    .map(col -> ChunkedColumnIterator.make(col, rowSetToUse))
                    .toArray(ColumnIterator[]::new);

            final RowSet.Iterator rsIterator = rowSetToUse.iterator();

            while (keyIterators[0].hasNext()) {
                final Object[] complexKey = Arrays.stream(keyIterators).map(ColumnIterator::next).toArray();
                result.put(complexKey, rsIterator.next());
            }

            SafeCloseableArray.close(keyIterators);
            rsIterator.close();

            return result;
        }
    }
}
