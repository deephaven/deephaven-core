/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
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
            return internalBlinkToAppendOnly(blinkTable);
        }
    }

    private static Table internalBlinkToAppendOnly(final Table blinkTable) {
        return QueryPerformanceRecorder.withNugget("blinkToAppendOnly", () -> {
            if (!isBlink(blinkTable)) {
                throw new IllegalArgumentException("Input is not a blink table!");
            }

            final BaseTable<?> baseBlinkTable = (BaseTable<?>) blinkTable.coalesce();
            final SwapListener swapListener = baseBlinkTable.createSwapListenerIfRefreshing(SwapListener::new);
            // blink tables must tick
            Assert.neqNull(swapListener, "swapListener");

            final Mutable<QueryTable> resultHolder = new MutableObject<>();

            ConstructSnapshot.callDataSnapshotFunction("blinkToAppendOnly", swapListener.makeSnapshotControl(),
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


                        final TrackingWritableRowSet rowSet;
                        if (usePrev) {
                            try (final RowSet useRowSet = baseBlinkTable.getRowSet().copyPrev()) {
                                rowSet = RowSetFactory.flat(useRowSet.size()).toTracking();
                                ChunkUtils.copyData(sourceColumns, useRowSet, destColumns, rowSet, usePrev);
                            }
                        } else {
                            rowSet = RowSetFactory.flat(baseBlinkTable.getRowSet().size()).toTracking();
                            ChunkUtils.copyData(sourceColumns, baseBlinkTable.getRowSet(), destColumns, rowSet,
                                    usePrev);
                        }

                        final QueryTable result = new QueryTable(rowSet, columns);
                        result.setRefreshing(true);
                        result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
                        result.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
                        result.setFlat();
                        resultHolder.setValue(result);

                        swapListener.setListenerAndResult(new BaseTable.ListenerImpl("streamToAppendOnly",
                                baseBlinkTable, result) {
                            @Override
                            public void onUpdate(TableUpdate upstream) {
                                if (upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
                                    throw new IllegalArgumentException("Blink tables should not modify or shift!");
                                }
                                final long newRows = upstream.added().size();
                                if (newRows == 0) {
                                    return;
                                }
                                final long currentSize = rowSet.size();
                                columns.values().forEach(c -> c.ensureCapacity(currentSize + newRows));

                                final RowSet newRange = RowSetFactory.fromRange(currentSize, currentSize + newRows - 1);

                                ChunkUtils.copyData(sourceColumns, upstream.added(), destColumns, newRange, false);
                                rowSet.insertRange(currentSize, currentSize + newRows - 1);

                                final TableUpdateImpl downstream = new TableUpdateImpl();
                                downstream.added = newRange;
                                downstream.modified = RowSetFactory.empty();
                                downstream.removed = RowSetFactory.empty();
                                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                                downstream.shifted = RowSetShiftData.EMPTY;
                                result.notifyListeners(downstream);
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
