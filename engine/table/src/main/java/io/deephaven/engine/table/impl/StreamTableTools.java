package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
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
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tools for manipulating tables.
 */
public class StreamTableTools {
    /**
     * Convert a Stream Table to an in-memory append only table.
     *
     * Note, this table will grow without bound as new stream values are encountered.
     *
     * @param streamTable the input stream table
     * @return an append-only in-memory table representing all data encountered in the stream
     */
    public static Table streamToAppendOnlyTable(final Table streamTable) {
        return QueryPerformanceRecorder.withNugget("streamToAppendOnlyTable", () -> {
            if (!isStream(streamTable)) {
                throw new IllegalArgumentException("Input is not a stream table!");
            }

            final BaseTable baseStreamTable = (BaseTable) streamTable.coalesce();

            final SwapListener swapListener =
                    baseStreamTable.createSwapListenerIfRefreshing(SwapListener::new);
            // stream tables must tick
            Assert.neqNull(swapListener, "swapListener");

            final Mutable<QueryTable> resultHolder = new MutableObject<>();

            ConstructSnapshot.callDataSnapshotFunction("streamToAppendOnlyTable", swapListener.makeSnapshotControl(),
                    (boolean usePrev, long beforeClockValue) -> {
                        final Map<String, ArrayBackedColumnSource<?>> columns = new LinkedHashMap<>();
                        final Map<String, ? extends ColumnSource<?>> columnSourceMap = streamTable.getColumnSourceMap();
                        final int columnCount = columnSourceMap.size();
                        final ColumnSource<?>[] sourceColumns = new ColumnSource[columnCount];
                        final WritableColumnSource<?>[] destColumns = new WritableColumnSource[columnCount];
                        int colIdx = 0;
                        for (Map.Entry<String, ? extends ColumnSource<?>> nameColumnSourceEntry : columnSourceMap
                                .entrySet()) {
                            final ColumnSource<?> existingColumn = nameColumnSourceEntry.getValue();
                            final ArrayBackedColumnSource<?> newColumn = ArrayBackedColumnSource.getMemoryColumnSource(
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
                            try (final RowSet useRowSet = baseStreamTable.getRowSet().copyPrev()) {
                                rowSet = RowSetFactory.flat(useRowSet.size()).toTracking();
                                ChunkUtils.copyData(sourceColumns, useRowSet, destColumns, rowSet, usePrev);
                            }
                        } else {
                            rowSet = RowSetFactory.flat(baseStreamTable.getRowSet().size())
                                    .toTracking();
                            ChunkUtils.copyData(sourceColumns, baseStreamTable.getRowSet(), destColumns, rowSet,
                                    usePrev);
                        }

                        final QueryTable result = new QueryTable(rowSet, columns);
                        result.setRefreshing(true);
                        result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
                        result.setFlat();
                        result.addParentReference(swapListener);
                        resultHolder.setValue(result);

                        swapListener.setListenerAndResult(new BaseTable.ListenerImpl("streamToAppendOnly",
                                streamTable, result) {
                            @Override
                            public void onUpdate(TableUpdate upstream) {
                                if (upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
                                    throw new IllegalArgumentException("Stream tables should not modify or shift!");
                                }
                                final long newRows = upstream.added().size();
                                if (newRows == 0) {
                                    return;
                                }
                                final long currentSize = rowSet.size();
                                columns.values().forEach(c -> c.ensureCapacity(currentSize + newRows));

                                final RowSet newRange =
                                        RowSetFactory.fromRange(currentSize,
                                                currentSize + newRows - 1);

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
     * Returns true if table is a stream table.
     *
     * @param table the table to check for stream behavior
     * @return Whether this table is a stream table
     * @see Table#STREAM_TABLE_ATTRIBUTE
     */
    public static boolean isStream(Table table) {
        if (!table.isRefreshing()) {
            return false;
        }
        return Boolean.TRUE.equals(table.getAttribute(Table.STREAM_TABLE_ATTRIBUTE));
    }
}
