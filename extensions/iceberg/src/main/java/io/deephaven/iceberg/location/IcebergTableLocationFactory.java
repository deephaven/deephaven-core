//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.impl.TableLocationFactory;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TableLocationFactory} for Iceberg {@link TableLocation}s.
 */
public final class IcebergTableLocationFactory implements TableLocationFactory<TableKey, IcebergTableLocationKey> {
    public IcebergTableLocationFactory() {}

    @Override
    @NotNull
    public TableLocation makeLocation(@NotNull final TableKey tableKey,
            @NotNull final IcebergTableLocationKey locationKey,
            @Nullable final TableDataRefreshService refreshService) {
        if (locationKey instanceof IcebergTableParquetLocationKey) {
            return new ParquetTableLocation(tableKey, (ParquetTableLocationKey) locationKey,
                    (ParquetInstructions) locationKey.readInstructions());
        }
        throw new UnsupportedOperationException("Unsupported location key type: " + locationKey.getClass());
    }
}
