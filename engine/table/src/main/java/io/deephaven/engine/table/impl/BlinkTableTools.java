/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tools for manipulating blink tables.
 *
 * @see Table#BLINK_TABLE_ATTRIBUTE
 */
public class BlinkTableTools {

    /**
     * Convert a Blink Table to an in-memory append only table.
     * <p>
     * Note, this table will grow without bound as new blink table rows are encountered.
     *
     * @param blinkTable The input blink table
     * @return An append-only in-memory table representing all data encountered in the blink table across all cycles
     */
    public static Table blinkToAppendOnly(final Table blinkTable) {
        final UpdateGraph updateGraph = blinkTable.getUpdateGraph();
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            // Setting the size limit as maximum allowed value
            return internalBlinkToAppendOnly(blinkTable, Long.MAX_VALUE);
        }
    }

    /**
     * Convert a Blink Table to an in-memory append only table with a limit on maximum size. Any updates beyond that
     * limit won't be appended to the table.
     *
     * @param blinkTable The input blink table
     * @param sizeLimit The maximum number of rows in the append-only table
     * @return An append-only in-memory table representing all data encountered in the blink table across all cycles
     *         till maximum row count
     */
    public static Table blinkToAppendOnly(final Table blinkTable, long sizeLimit) {
        if (sizeLimit < 0) {
            throw new IllegalArgumentException("Size limit cannot be negative, limit=" + sizeLimit);
        }
        final UpdateGraph updateGraph = blinkTable.getUpdateGraph();
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            return internalBlinkToAppendOnly(blinkTable, sizeLimit);
        }
    }

    private static Table internalBlinkToAppendOnly(final Table blinkTable, long sizeLimit) {
        return QueryPerformanceRecorder.withNugget("blinkToAppendOnly", () -> {
            if (!isBlink(blinkTable)) {
                throw new IllegalArgumentException("Input is not a blink table!");
            }

            final BaseTable<?> baseBlinkTable = (BaseTable<?>) blinkTable.coalesce();
            final OperationSnapshotControl snapshotControl =
                    baseBlinkTable.createSnapshotControlIfRefreshing(OperationSnapshotControl::new);
            // blink tables must tick
            Assert.neqNull(snapshotControl, "snapshotControl");

            final Mutable<QueryTable> resultHolder = new MutableObject<>();

            ConstructSnapshot.callDataSnapshotFunction("blinkToAppendOnly", snapshotControl,
                    (boolean usePrev, long beforeClockValue) -> {
                        final Map<String, WritableColumnSource<?>> columns = new LinkedHashMap<>();
                        final Map<String, ? extends ColumnSource<?>> columnSourceMap =
                                baseBlinkTable.getColumnSourceMap();
                        final int columnCount = columnSourceMap.size();
                        final ColumnSource<?>[] sourceColumns = new ColumnSource[columnCount];
                        final WritableColumnSource<?>[] destColumns = new WritableColumnSource[columnCount];
                        int colIdx = 0;
                        for (Map.Entry<String, ? extends ColumnSource<?>> nameColumnSourceEntry : columnSourceMap
                                .entrySet()) {
                            final ColumnSource<?> existingColumn = nameColumnSourceEntry.getValue();
                            final WritableColumnSource<?> newColumn = ArrayBackedColumnSource.getMemoryColumnSource(
                                    0, existingColumn.getType(), existingColumn.getComponentType());
                            columns.put(nameColumnSourceEntry.getKey(), newColumn);
                            // for the source columns, we would like to read primitives instead of objects in cases
                            // where it is possible
                            sourceColumns[colIdx] = ReinterpretUtils.maybeConvertToPrimitive(existingColumn);
                            // for the destination sources, we know they are array backed sources that will actually
                            // store primitives and we can fill efficiently
                            destColumns[colIdx++] =
                                    (WritableColumnSource<?>) ReinterpretUtils.maybeConvertToPrimitive(newColumn);
                        }

                        final RowSet baseRowSet =
                                (usePrev ? baseBlinkTable.getRowSet().prev() : baseBlinkTable.getRowSet());
                        final RowSet useRowSet;
                        if (baseRowSet.size() > sizeLimit) {
                            useRowSet = baseRowSet.subSetByPositionRange(0, sizeLimit);
                        } else {
                            useRowSet = baseRowSet;
                        }
                        final TrackingWritableRowSet rowSet = RowSetFactory.flat(useRowSet.size()).toTracking();
                        ChunkUtils.copyData(sourceColumns, useRowSet, destColumns, rowSet, usePrev);

                        final QueryTable result = new QueryTable(rowSet, columns);
                        result.setRefreshing(true);
                        result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
                        result.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
                        result.setFlat();
                        resultHolder.setValue(result);

                        Assert.leq(result.size(), "result.size()", sizeLimit, "sizeLimit");

                        snapshotControl.setListenerAndResult(new BaseTable.ListenerImpl("streamToAppendOnly",
                                baseBlinkTable, result) {
                            @Override
                            public void onUpdate(TableUpdate upstream) {
                                if (upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
                                    throw new IllegalArgumentException("Blink tables should not modify or shift!");
                                }
                                long newRowsSize = upstream.added().size();
                                if (newRowsSize == 0) {
                                    return;
                                }
                                RowSet subsetAdded = null;
                                final long currentSize = rowSet.size();
                                if (currentSize + newRowsSize >= sizeLimit) {
                                    newRowsSize = (sizeLimit - currentSize);
                                    subsetAdded = upstream.added().subSetByPositionRange(0, newRowsSize);
                                }
                                final long totalSize = currentSize + newRowsSize;
                                columns.values().forEach(c -> c.ensureCapacity(totalSize));
                                final RowSet newRange = RowSetFactory.fromRange(currentSize, totalSize - 1);

                                try (final SafeCloseable ignored = subsetAdded) {
                                    final RowSet newRowSet = (subsetAdded == null) ? upstream.added() : subsetAdded;
                                    ChunkUtils.copyData(sourceColumns, newRowSet, destColumns, newRange, false);
                                }
                                rowSet.insertRange(currentSize, totalSize - 1);
                                Assert.leq(totalSize, "totalSize", sizeLimit, "sizeLimit");

                                final TableUpdateImpl downstream = new TableUpdateImpl();
                                downstream.added = newRange;
                                downstream.modified = RowSetFactory.empty();
                                downstream.removed = RowSetFactory.empty();
                                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                                downstream.shifted = RowSetShiftData.EMPTY;
                                result.notifyListeners(downstream);

                                if (totalSize == sizeLimit) {
                                    // No more rows can be appended, so remove the listener and remove all references
                                    forceReferenceCountToZero();
                                }
                            }
                        }, result);

                        return true;
                    });

            return resultHolder.getValue();
        });
    }

    /**
     * Returns true if table is a blink table.
     *
     * @param table The table to check for blink behavior
     * @return Whether this table is a blink table
     * @see Table#BLINK_TABLE_ATTRIBUTE
     */
    public static boolean isBlink(Table table) {
        if (!table.isRefreshing()) {
            return false;
        }
        return Boolean.TRUE.equals(table.getAttribute(Table.BLINK_TABLE_ATTRIBUTE));
    }
}
