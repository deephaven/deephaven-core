//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.impl.TableLocationFactory;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TableLocationFactory} for {@link IcebergTableLocation}s.
 */
public final class IcebergTableLocationFactory implements TableLocationFactory<TableKey, IcebergTableLocationKey> {
    private final Object readInstructions;

    public IcebergTableLocationFactory(@NotNull final Object readInstructions) {
        this.readInstructions = readInstructions;
    }

    @Override
    @NotNull
    public TableLocation makeLocation(@NotNull final TableKey tableKey,
            @NotNull final IcebergTableLocationKey locationKey,
            @Nullable final TableDataRefreshService refreshService) {
        return new IcebergTableLocation(tableKey, locationKey, readInstructions);
    }
}
