package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import org.jetbrains.annotations.NotNull;

public interface DataIndexBuilder<DATA_TYPE> {

    void addSource(final int regionIndex,
            @NotNull final ColumnLocation columnLocation,
            @NotNull final RowSet locationRowSetInTable);

    ColumnDefinition<DATA_TYPE> getColumnDefinition();


    static <DATA_TYPE> PartitioningIndexProvider<DATA_TYPE> makeBuilder(
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition) {
        if (columnDefinition.isPartitioning()) {
            return new PartitioningIndexProvider<>(columnDefinition);
        }
        return new PartitioningIndexProvider<>(columnDefinition);
    }
}
