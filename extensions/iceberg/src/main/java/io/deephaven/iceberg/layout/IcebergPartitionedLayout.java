//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@InternalUseOnly
public final class IcebergPartitionedLayout extends IcebergBaseLayout {

    private final List<PartitionField> fields;

    public IcebergPartitionedLayout(
            @NotNull IcebergTableAdapter tableAdapter,
            @NotNull ParquetInstructions parquetInstructions,
            @NotNull SeekableChannelsProvider seekableChannelsProvider,
            @Nullable Snapshot snapshot,
            @NotNull List<PartitionField> fields) {
        super(tableAdapter, parquetInstructions, seekableChannelsProvider, snapshot);
        this.fields = Objects.requireNonNull(fields);
    }

    @Override
    public String toString() {
        return IcebergPartitionedLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(
            @NotNull final ManifestFile manifestFile,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            @NotNull final SeekableChannelsProvider channelsProvider) {



        manifestFile.partitionSpecId();
        dataFile.partition();




//        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
//
//        final PartitionData partitionData = (PartitionData) dataFile.partition();
//        for (final IdentityPartitioningColData colData : identityPartitioningColumns) {
//            final String colName = colData.name;
//            final Object colValue;
//            final Object valueFromPartitionData = partitionData.get(colData.index);
//            if (valueFromPartitionData != null) {
//                colValue = IdentityPartitionConverters.convertConstant(
//                        partitionData.getType(colData.index), valueFromPartitionData);
//                if (!colData.type.isAssignableFrom(colValue.getClass())) {
//                    throw new TableDataException("Partitioning column " + colName
//                            + " has type " + colValue.getClass().getName()
//                            + " but expected " + colData.type.getName());
//                }
//            } else {
//                colValue = null;
//            }
//            partitions.put(colName, (Comparable<?>) colValue);
//        }
//        return locationKey(manifestFile, dataFile, fileUri, partitions, channelsProvider);
    }
}
