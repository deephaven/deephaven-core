package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.impl.NonexistentTableLocation;
import io.deephaven.engine.table.impl.locations.impl.TableLocationFactory;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

/**
 * {@link TableLocationFactory} for {@link ParquetTableLocation}s.
 */
public final class ParquetTableLocationFactory implements TableLocationFactory<TableKey, ParquetTableLocationKey> {

    private final ParquetInstructions readInstructions;

    public ParquetTableLocationFactory(@NotNull final ParquetInstructions readInstructions) {
        this.readInstructions = readInstructions;
    }

    @Override
    @NotNull
    public TableLocation makeLocation(@NotNull final TableKey tableKey,
            @NotNull final ParquetTableLocationKey locationKey,
            @Nullable final TableDataRefreshService refreshService) {
        final File parquetFile = locationKey.getFile();
        if (Utils.fileExistsPrivileged(parquetFile)) {
            return new ParquetTableLocation(tableKey, locationKey, readInstructions);
        } else {
            return new NonexistentTableLocation(tableKey, locationKey);
        }
    }
}
