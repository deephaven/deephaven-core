package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.table.impl.dataindex.BaseDataIndex.INDEX_COL_NAME;

/**
 * This class will provide methods to build a deferred single-column index (i.e. grouping) for a partitioning
 * storage-backed table.
 */
public class PartitioningIndexProvider<DATA_TYPE> implements DataIndexBuilder<DATA_TYPE> {
    private final ColumnDefinition<DATA_TYPE> columnDefinition;

    private final WritableColumnSource<DATA_TYPE> valueSource;
    private final ObjectArraySource<RowSet> rowSetSource;

    private final Table locationTable;

    public PartitioningIndexProvider(@NotNull final ColumnDefinition<DATA_TYPE> columnDefinition) {
        this.columnDefinition = columnDefinition;
        valueSource = ArrayBackedColumnSource.getMemoryColumnSource(10, columnDefinition.getDataType(), null);
        rowSetSource =
                (ObjectArraySource<RowSet>) ArrayBackedColumnSource.getMemoryColumnSource(10, RowSet.class, null);

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        columnSourceMap.put(columnDefinition.getName(), valueSource);
        columnSourceMap.put(INDEX_COL_NAME, rowSetSource);
        locationTable = new QueryTable(RowSetFactory.empty().toTracking(), columnSourceMap);
    }

    @Override
    public void addSource(final int regionIndex,
            @NotNull final ColumnLocation columnLocation,
            @NotNull final RowSet locationRowSetInTable) {
        valueSource.ensureCapacity(regionIndex + 1);
        rowSetSource.ensureCapacity(regionIndex + 1);
        final DATA_TYPE columnPartitionValue =
                columnLocation.getTableLocation().getKey().getPartitionValue(columnDefinition.getName());
        valueSource.set(regionIndex, columnPartitionValue);
        rowSetSource.set(regionIndex, locationRowSetInTable);
    }

    @Override
    public ColumnDefinition<DATA_TYPE> getColumnDefinition() {
        return columnDefinition;
    }

    public Table table() {
        return locationTable;
    }
}
