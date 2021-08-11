package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReinterpretUtilities;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;

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
    public static Table streamToAppendOnlyTable(Table streamTable) {
        return QueryPerformanceRecorder.withNugget("streamToAppendOnlyTable", () -> {
            if (!isStream(streamTable)) {
                throw new IllegalArgumentException("Input is not a stream table!");
            }

            final Map<String, ArrayBackedColumnSource> columns = new LinkedHashMap<>();
            final Map<String, ? extends ColumnSource> columnSourceMap = streamTable.getColumnSourceMap();
            final int columnCount = columnSourceMap.size();
            final ColumnSource [] sourceColumns = new ColumnSource[columnCount];
            final WritableSource[] destColumns = new WritableSource[columnCount];
            int colIdx = 0;
            for (Map.Entry<String, ? extends ColumnSource> nameColumnSourceEntry : columnSourceMap.entrySet()) {
                final ColumnSource<?> existingColumn = nameColumnSourceEntry.getValue();
                final ArrayBackedColumnSource<?> newColumn = ArrayBackedColumnSource.getMemoryColumnSource(0, existingColumn.getType(), existingColumn.getComponentType());
                columns.put(nameColumnSourceEntry.getKey(), newColumn);
                // for the source columns, we would like to read primitives instead of objects in cases where it is possible
                sourceColumns[colIdx] = ReinterpretUtilities.maybeConvertToPrimitive(existingColumn);
                // for the destination sources, we know they are array backed sources that will actually store primitives and we can fill efficiently
                destColumns[colIdx++] = (WritableSource)ReinterpretUtilities.maybeConvertToPrimitive(newColumn);
            }

            Index index = Index.FACTORY.getEmptyIndex();
            final QueryTable result = new QueryTable(index, columns);
            result.setRefreshing(true);
            result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
            result.setFlat();

            ((DynamicTable) streamTable).listenForUpdates(new BaseTable.ShiftAwareListenerImpl("streamToAppendOnly", (DynamicTable) streamTable, result) {
                @Override
                public void onUpdate(Update upstream) {
                    if (upstream.modified.nonempty() || upstream.shifted.nonempty()) {
                        throw new IllegalArgumentException("Stream tables should not modify or shift!");
                    }
                    final long newRows = upstream.added.size();
                    if (newRows == 0) {
                        return;
                    }
                    final long currentSize = index.size();
                    columns.values().forEach(c -> c.ensureCapacity(currentSize + newRows));

                    final Index newRange = Index.CURRENT_FACTORY.getIndexByRange(currentSize, currentSize + newRows - 1);

                    ChunkUtils.copyData(sourceColumns, upstream.added, destColumns, newRange, false);
                    index.insertRange(currentSize, currentSize + newRows - 1);

                    final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();
                    downstream.added = newRange;
                    downstream.modified = Index.CURRENT_FACTORY.getEmptyIndex();
                    downstream.removed = Index.CURRENT_FACTORY.getEmptyIndex();
                    downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                    downstream.shifted = IndexShiftData.EMPTY;
                    result.notifyListeners(downstream);
                }
            });
            return result;
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
        if (!table.isLive()) {
            return false;
        }
        return Boolean.TRUE.equals(table.getAttribute(Table.STREAM_TABLE_ATTRIBUTE));
    }
}
