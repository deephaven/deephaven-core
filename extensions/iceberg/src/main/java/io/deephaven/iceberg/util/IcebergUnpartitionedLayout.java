//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.iceberg.layout.IcebergBaseLayout;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

final class IcebergUnpartitionedLayout extends IcebergBaseLayout {

    IcebergUnpartitionedLayout(
            @NotNull IcebergTableAdapter tableAdapter,
            @NotNull ParquetInstructions parquetInstructions,
            @NotNull SeekableChannelsProvider seekableChannelsProvider,
            @Nullable Snapshot snapshot) {
        super(tableAdapter, parquetInstructions, seekableChannelsProvider, snapshot);
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

    @Override
    public String toString() {
        return IcebergUnpartitionedLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }
}
