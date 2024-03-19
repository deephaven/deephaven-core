//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.OperationSnapshotControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.RowSetColumnSourceWrapper;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Abstract base implementation of {@link DataIndex} that provides common functionality.
 */
public abstract class AbstractDataIndex extends LivenessArtifact implements DataIndex {

    /**
     * The name of the {@link RowSet} column in the index {@link #table()} used by all {@link AbstractDataIndex}
     * instances.
     */
    protected static final String ROW_SET_COLUMN_NAME = "dh_row_set";

    @Override
    @NotNull
    public final String rowSetColumnName() {
        return ROW_SET_COLUMN_NAME;
    }

    @Override
    @NotNull
    public final BasicDataIndex transform(@NotNull final DataIndexTransformer transformer) {
        return TransformedDataIndex.from(this, transformer);
    }

    @Override
    @NotNull
    public final DataIndex remapKeyColumns(@NotNull final Map<ColumnSource<?>, ColumnSource<?>> oldToNewColumnMap) {
        return RemappedDataIndex.from(this, oldToNewColumnMap);
    }

    /**
     * Whether this AbstractDataIndex is potentially usable. This will return {@code true} when there are no known
     * issues for this data index. This performs fast checks, such as verifying all locations have index table files,
     * but does not fully guarantee that the index is complete and loadable.
     *
     * @return true If the AbstractDataIndex is potentially usable, false otherwise
     */
    public abstract boolean isValid();

    /**
     * Return a copy of {@code parent} with the {@link RowSet} column replaced with a {@link RowSetColumnSourceWrapper
     * wrapper} column that adds {@link TrackingRowSet#prev() prev} calls on access to previous values.
     *
     * @param parent The {@link Table} to copy
     * @param rowSetColumn The name of the {@link RowSet} column to wrap
     * @return The copied {@link Table}
     */
    @SuppressWarnings("unused")
    protected static QueryTable indexTableWrapper(
            @NotNull final QueryTable parent,
            @NotNull final String rowSetColumn) {
        return indexTableWrapper(parent, rowSetColumn, rowSetColumn);
    }

    /**
     * Return a copy of {@code parent} with the {@link RowSet} column replaced with a {@link RowSetColumnSourceWrapper
     * wrapper} column that adds {@link TrackingRowSet#prev() prev} calls on access to previous values.
     *
     * @param parent The {@link Table} to copy
     * @param rowSetColumn The name of the {@link RowSet} column to wrap
     * @param renamedRowSetColumn The name of the {@link RowSet} column in the output {@link Table}
     * @return The copied {@link Table}
     */
    protected static QueryTable indexTableWrapper(
            @NotNull final QueryTable parent,
            @NotNull final String rowSetColumn,
            @NotNull final String renamedRowSetColumn) {
        parent.getDefinition().checkHasColumn(rowSetColumn, RowSet.class);
        if (!rowSetColumn.equals(renamedRowSetColumn) &&
                parent.getDefinition().getColumnNameSet().contains(renamedRowSetColumn)) {
            throw new IllegalArgumentException(String.format(
                    "Cannot rename %s to %s, table already contains a column named %s in %s",
                    rowSetColumn, renamedRowSetColumn, renamedRowSetColumn, parent.getDefinition().getColumnNames()));
        }

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
                        resultColumnSourceMap.put(columnName, columnSource);
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
}
