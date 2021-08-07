package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.OrderedKeys;

import java.util.LinkedHashMap;
import java.util.Map;

public class StreamTableToAppendOnlyTable {
    public static Table streamToAppendOnlyTable(Table streamTable) {
        return QueryPerformanceRecorder.withNugget("streamToAppendOnlyTable", () -> {
            Object streamAttribute = streamTable.getAttribute(Table.STREAM_TABLE_ATTRIBUTE);
            if (streamAttribute == null || !(streamAttribute instanceof Boolean) || !(Boolean)streamAttribute) {
                throw new IllegalArgumentException("Input is not a stream table!");
            }

            final Map<String, ArrayBackedColumnSource> columns = new LinkedHashMap<>();
            final int columnCount = streamTable.getColumnSourceMap().size();
            final ColumnSource [] sourceColumns = new ColumnSource[columnCount];
            final ArrayBackedColumnSource [] destColumns = new ArrayBackedColumnSource[columnCount];
            int colIdx = 0;
            for (Map.Entry<String, ? extends ColumnSource> nameColumnSourceEntry : streamTable.getColumnSourceMap().entrySet()) {
                final ColumnSource<?> existingColumn = nameColumnSourceEntry.getValue();
                final ArrayBackedColumnSource<?> newColumn = ArrayBackedColumnSource.getMemoryColumnSource(0, existingColumn.getType(), existingColumn.getComponentType());
                columns.put(nameColumnSourceEntry.getKey(), newColumn);
                sourceColumns[colIdx] = existingColumn;
                destColumns[colIdx++] = newColumn;
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

                    final Index newRange = Index.FACTORY.getIndexByRange(currentSize, currentSize + newRows - 1);

                    for (int cc = 0; cc < columnCount; ++cc) {
                        ChunkUtils.copyData(sourceColumns[cc], upstream.added, destColumns[cc], newRange, false);
                    }
                    index.insertRange(currentSize, currentSize + newRows - 1);

                    final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();
                    downstream.shifted = IndexShiftData.EMPTY;
                    downstream.modified = Index.FACTORY.getEmptyIndex();
                    downstream.removed = Index.FACTORY.getEmptyIndex();
                    downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                    downstream.added = newRange;
                    result.notifyListeners(downstream);
                }
            });
            return result;
        });
    }
}
