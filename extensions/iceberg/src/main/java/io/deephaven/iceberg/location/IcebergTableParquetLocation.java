//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;

/**
 * {@link TableLocation} implementation for use with data stored in Iceberg tables in the parquet format.
 */
public class IcebergTableParquetLocation extends ParquetTableLocation {
    private static final String IMPLEMENTATION_NAME = IcebergTableParquetLocation.class.getSimpleName();

    public IcebergTableParquetLocation(@NotNull final TableKey tableKey,
            @NotNull final IcebergTableLocationKey tableLocationKey) {
        super(tableKey, (ParquetTableLocationKey) tableLocationKey,
                (ParquetInstructions) tableLocationKey.readInstructions());
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }
}
