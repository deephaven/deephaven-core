//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergInstructions;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;

import java.net.URI;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables without partitions that will discover data files
 * from a {@link Snapshot}
 */
public final class IcebergFlatLayout extends IcebergBaseLayout {
    /**
     * @param tableDef The {@link TableDefinition} that will be used for the table.
     * @param table The {@link Table} to discover locations for.
     * @param tableSnapshot The {@link Snapshot} from which to discover data files.
     * @param fileIO The file IO to use for reading manifest data files.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergFlatLayout(
            @NotNull final TableDefinition tableDef,
            @NotNull final Table table,
            @NotNull final Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final IcebergInstructions instructions) {
        super(tableDef, table, tableSnapshot, fileIO, instructions);
    }

    @Override
    public String toString() {
        return IcebergFlatLayout.class.getSimpleName() + '[' + table.name() + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(DataFile df, URI fileUri) {
        return locationKey(df.format(), fileUri, null);
    }
}
