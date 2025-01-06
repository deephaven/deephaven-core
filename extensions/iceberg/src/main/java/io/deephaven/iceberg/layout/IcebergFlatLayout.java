//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import org.apache.iceberg.*;
import org.jetbrains.annotations.NotNull;

import java.net.URI;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables without partitions that will discover data files
 * from a {@link Snapshot}
 */
public final class IcebergFlatLayout extends IcebergBaseLayout {
    /**
     * @param tableAdapter The {@link IcebergTableAdapter} that will be used to access the table.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergFlatLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final IcebergReadInstructions instructions,
            @NotNull final DataInstructionsProviderLoader dataInstructionsProvider) {
        super(tableAdapter, instructions, dataInstructionsProvider);
    }

    @Override
    public String toString() {
        return IcebergFlatLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(
            @NotNull final ManifestFile manifestFile,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri) {
        return locationKey(manifestFile, dataFile, fileUri, null);
    }
}
