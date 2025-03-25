//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.*;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.*;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables with partitions that will discover data files from
 * a {@link Snapshot}
 */
@InternalUseOnly
public final class IcebergKeyValuePartitionedLayout extends IcebergBaseLayout {

    @InternalUseOnly
    public static class IdentityPartitioningColData {
        final String name;
        final Class<?> type;
        final int index; // position in the partition spec

        public IdentityPartitioningColData(String name, Class<?> type, int index) {
            this.name = name;
            this.type = type;
            this.index = index;
        }
    }

    private final List<IdentityPartitioningColData> identityPartitioningColumns;

    public IcebergKeyValuePartitionedLayout(
            @NotNull IcebergTableAdapter tableAdapter,
            @NotNull ParquetInstructions parquetInstructions,
            @NotNull SeekableChannelsProvider seekableChannelsProvider,
            @Nullable Snapshot snapshot,
            @NotNull List<IdentityPartitioningColData> identityPartitioningColumns) {
        super(tableAdapter, parquetInstructions, seekableChannelsProvider, snapshot);
        this.identityPartitioningColumns = Objects.requireNonNull(identityPartitioningColumns);
    }

    @Override
    public String toString() {
        return IcebergKeyValuePartitionedLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(
            @NotNull final ManifestFile manifestFile,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();

        final PartitionData partitionData = (PartitionData) dataFile.partition();
        for (final IdentityPartitioningColData colData : identityPartitioningColumns) {
            final String colName = colData.name;
            final Object colValue;
            final Object valueFromPartitionData = partitionData.get(colData.index);
            if (valueFromPartitionData != null) {
                colValue = IdentityPartitionConverters.convertConstant(
                        partitionData.getType(colData.index), valueFromPartitionData);
                if (!colData.type.isAssignableFrom(colValue.getClass())) {
                    throw new TableDataException("Partitioning column " + colName
                            + " has type " + colValue.getClass().getName()
                            + " but expected " + colData.type.getName());
                }
            } else {
                colValue = null;
            }
            partitions.put(colName, (Comparable<?>) colValue);
        }
        return locationKey(manifestFile, dataFile, fileUri, partitions, channelsProvider);
    }
}
