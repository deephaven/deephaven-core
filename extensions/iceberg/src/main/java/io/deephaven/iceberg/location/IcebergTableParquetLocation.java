//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.api.SortColumn;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public class IcebergTableParquetLocation extends ParquetTableLocation implements TableLocation {

    @Nullable
    private final List<SortColumn> sortedColumns;

    public IcebergTableParquetLocation(
            @NotNull final TableKey tableKey,
            @NotNull final IcebergTableParquetLocationKey tableLocationKey,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, readInstructions);
        sortedColumns = Require.neqNull(tableLocationKey.sortedColumns(), "tableLocationKey.sortedColumns()");
    }

    @Override
    @NotNull
    public List<SortColumn> getSortedColumns() {
        return sortedColumns;
    }
}
