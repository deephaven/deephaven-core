//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.impl.NonexistentTableLocation;
import io.deephaven.engine.table.impl.locations.impl.TableLocationFactory;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.parquet.table.ParquetInstructions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.net.URI;

import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;

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
        final URI parquetFileURI = locationKey.getURI();
        if (!FILE_URI_SCHEME.equals(parquetFileURI.getScheme()) || new File(parquetFileURI).exists()) {
            return new ParquetTableLocation(tableKey, locationKey, readInstructions);
        } else {
            return new NonexistentTableLocation(tableKey, locationKey);
        }
    }
}
