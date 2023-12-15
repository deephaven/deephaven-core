package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class LocationDataIndex extends LivenessArtifact implements BasicDataIndex {

    private final Table table;
    private final String[] keyColumnNames;
    private final String rowSetColumnName;

    public static LocationDataIndex from(
            @NotNull final Table table,
            @NotNull final String[] keyColumnNames,
            @NotNull final String rowSetColumnName) {
        return new LocationDataIndex(table, keyColumnNames, rowSetColumnName);
    }

    private LocationDataIndex(
            @NotNull final Table table,
            @NotNull final String[] keyColumnNames,
            @NotNull final String rowSetColumnName) {
        this.table = table;
        this.keyColumnNames = keyColumnNames;
        this.rowSetColumnName = rowSetColumnName;
    }

    @Override
    public String[] keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnMap() {
        throw new UnsupportedOperationException("LocationDataIndex#keyColumnMap");
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
        return false;
    }
}
