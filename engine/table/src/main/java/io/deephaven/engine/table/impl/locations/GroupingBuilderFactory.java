package io.deephaven.engine.table.impl.locations;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.GroupingProvider;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A builder interface used to create grouping tables or maps.
 */
public interface GroupingBuilderFactory {
    static <DATA_TYPE> Table makeNullOnlyGroupingTable(final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final RowSet locationRowSetInTable) {
        final NullValueColumnSource<DATA_TYPE> nullOnlyColumn =
                NullValueColumnSource.getInstance(columnDefinition.getDataType(), null);
        final SingleValueColumnSource<RowSet> indexColumn =
                SingleValueColumnSource.getSingleValueColumnSource(RowSet.class);
        indexColumn.set(locationRowSetInTable);

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        columnSourceMap.put(columnDefinition.getName(), nullOnlyColumn);
        columnSourceMap.put(GroupingProvider.INDEX_COL_NAME, indexColumn);

        return new QueryTable(RowSetFactory.flat(1).toTracking(), columnSourceMap);
    }
}
