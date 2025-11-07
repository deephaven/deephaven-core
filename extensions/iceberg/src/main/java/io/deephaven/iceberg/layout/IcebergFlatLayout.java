//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables without partitions that will discover data files
 * from a {@link Snapshot}
 */
@InternalUseOnly
public final class IcebergFlatLayout extends IcebergBaseLayout {

    /**
     * @param tableAdapter The {@link IcebergTableAdapter} that will be used to access the table.
     * @param instructions The instructions for customizations while reading.
     * @param dataInstructionsProvider The provider for special instructions, to be used if special instructions not
     *        provided in the {@code instructions}.
     */
    // TODO(DH-19072): Refactor Iceberg TLKFs to reduce visibility
    @Deprecated(forRemoval = true)
    public IcebergFlatLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final IcebergReadInstructions instructions,
            @NotNull final DataInstructionsProviderLoader dataInstructionsProvider) {
        super(tableAdapter, instructions, dataInstructionsProvider);
    }

    // TODO(DH-19072): Refactor Iceberg TLKFs to reduce visibility
    @Deprecated(forRemoval = true)
    public IcebergFlatLayout(
            @NotNull IcebergTableAdapter tableAdapter,
            @NotNull ParquetInstructions parquetInstructions,
            @NotNull SeekableChannelsProvider seekableChannelsProvider,
            @Nullable Snapshot snapshot) {
        super(tableAdapter, parquetInstructions, seekableChannelsProvider, snapshot);
    }

    @Override
    public String toString() {
        return IcebergFlatLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    @Override
    protected IcebergTableLocationKey keyFromDataFile(
            @NotNull final PartitionSpec manifestPartitionSpec,
            @NotNull final ManifestFile manifestFile,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        return locationKey(manifestPartitionSpec, manifestFile, dataFile, fileUri, null, channelsProvider);
    }
}
