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
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables without partitions that will discover data files
 * from a {@link Snapshot}
 */
public final class IcebergFlatLayout extends IcebergBaseLayout {
    /**
     * @param tableAdapter The {@link IcebergTableAdapter} that will be used to access the table.
     * @param tableSnapshot The {@link Snapshot} from which to discover data files.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergFlatLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @Nullable final Snapshot tableSnapshot,
            @NotNull final IcebergReadInstructions instructions,
            @NotNull final DataInstructionsProviderLoader dataInstructionsProvider) {
        super(tableAdapter, tableSnapshot, instructions, dataInstructionsProvider);
    }

    @Override
    public String toString() {
        return IcebergFlatLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(DataFile df, URI fileUri) {
        return locationKey(df.format(), fileUri, null);
    }
}
