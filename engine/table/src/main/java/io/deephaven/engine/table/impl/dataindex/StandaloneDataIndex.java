//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

/**
 * {@link BasicDataIndex} implementation that holds an index {@link Table} and does not specify the {@link ColumnSource
 * ColumnSources} that were indexed, and hence cannot support {@link #keyColumnNamesByIndexedColumn()}. This is useful
 * for standalone indices that are not associated with a specific table, but rather used to accumulate a merged index
 * for a merged table over the indexed data.
 */
public class StandaloneDataIndex extends LivenessArtifact implements BasicDataIndex {

    private final Table table;
    private final List<String> keyColumnNames;
    private final String rowSetColumnName;

    public static StandaloneDataIndex from(
            @NotNull final Table table,
            @NotNull final String[] keyColumnNames,
            @NotNull final String rowSetColumnName) {
        return new StandaloneDataIndex(table.coalesce(), keyColumnNames, rowSetColumnName);
    }

    private StandaloneDataIndex(
            @NotNull final Table table,
            @NotNull final String[] keyColumnNames,
            @NotNull final String rowSetColumnName) {
        this.table = table;
        this.keyColumnNames = List.of(keyColumnNames);
        this.rowSetColumnName = rowSetColumnName;
        if (table.isRefreshing()) {
            manage(table);
        }
    }

    @Override
    @NotNull
    public List<String> keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
        throw new UnsupportedOperationException("Cannot provide a key column map for a standalone data index");
    }

    @Override
    @NotNull
    public String rowSetColumnName() {
        return rowSetColumnName;
    }

    @Override
    @NotNull
    public Table table() {
        return table;
    }

    @Override
    public boolean isRefreshing() {
        return table.isRefreshing();
    }
}
