//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables without partitions that will discover data files
 * from a {@link Snapshot}
 */
public final class IcebergFlatLayout extends IcebergBaseLayout {

    public IcebergFlatLayout(
            @NotNull IcebergTableAdapter tableAdapter,
            @Nullable Snapshot snapshot,
            @NotNull ParquetInstructions pi,
            @NotNull SeekableChannelsProvider channelsProvider) {
        super(tableAdapter, snapshot, pi, channelsProvider);
    }

    @Override
    public String toString() {
        return IcebergFlatLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(
            @NotNull final ManifestFile manifestFile,
            @NotNull final PartitionSpec spec,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri) {
        return locationKey(manifestFile, spec, dataFile, fileUri, null);
    }
}
