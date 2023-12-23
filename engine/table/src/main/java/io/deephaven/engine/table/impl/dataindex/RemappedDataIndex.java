package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

public class RemappedDataIndex extends BaseDataIndex {
    private final BaseDataIndex sourceIndex;
    private final Map<ColumnSource<?>, String> columnNameMap;

    public RemappedDataIndex(final BaseDataIndex sourceIndex,
            final @NotNull Map<ColumnSource<?>, ColumnSource<?>> oldToNewColumnMap) {
        this.sourceIndex = sourceIndex;
        // Build a new map of column sources to index table key column names using either the original column
        // sources or the remapped column sources.
        columnNameMap = new LinkedHashMap<>();

        for (Map.Entry<ColumnSource<?>, String> entry : sourceIndex.keyColumnMap().entrySet()) {
            final ColumnSource<?> originalColumnSource = entry.getKey();
            // Use the remapped column source (or the original source if not remapped) as the key.
            columnNameMap.put(
                    oldToNewColumnMap.getOrDefault(originalColumnSource, originalColumnSource),
                    entry.getValue());
        }
    }

    @Override
    public String[] keyColumnNames() {
        return sourceIndex.keyColumnNames();
    }

    @Override
    public Map<ColumnSource<?>, String> keyColumnMap() {
        return columnNameMap;
    }

    @Override
    public String rowSetColumnName() {
        return sourceIndex.rowSetColumnName();
    }

    @Override
    @NotNull
    public Table table() {
        return sourceIndex.table();
    }

    @Override
    @NotNull
    public RowKeyLookup rowKeyLookup() {
        return sourceIndex.rowKeyLookup();
    }

    @Override
    public boolean isRefreshing() {
        return sourceIndex.isRefreshing();
    }

    @Override
    public boolean isValid() {
        return sourceIndex.isValid();
    }
}
