//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import org.jetbrains.annotations.NotNull;

import java.util.List;

class IcebergTableParquetLocation extends ParquetTableLocation implements TableLocation {

    IcebergTableParquetLocation(
            @NotNull final TableKey tableKey,
            @NotNull final IcebergTableParquetLocationKey tableLocationKey,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, readInstructions);
    }

    @Override
    @NotNull
    public List<SortColumn> getSortedColumns() {
        return ((IcebergTableParquetLocationKey) getKey()).sortedColumns();
    }
}
