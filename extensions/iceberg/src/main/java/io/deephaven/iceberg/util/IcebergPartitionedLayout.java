//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.iceberg.layout.IcebergBaseLayout;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class IcebergPartitionedLayout extends IcebergBaseLayout {

    private final Map<String, PartitionField> partitionFields;

    IcebergPartitionedLayout(
            @NotNull IcebergTableAdapter tableAdapter,
            @NotNull ParquetInstructions parquetInstructions,
            @NotNull SeekableChannelsProvider seekableChannelsProvider,
            @Nullable Snapshot snapshot,
            @NotNull Map<String, PartitionField> partitionFields) {
        super(tableAdapter, parquetInstructions, seekableChannelsProvider, snapshot);
        this.partitionFields = Objects.requireNonNull(partitionFields);
    }

    private static Object get(PartitionField partitionField, PartitionData data) {
        // Note: we could compute this mapping once per ManifestFile (give they are supposed to have the same partition
        // spec) but right now we are just doing it on-demand per data file.
        final List<Types.NestedField> fields = data.getPartitionType().fields();
        final int size = fields.size();
        int ix;
        for (ix = 0; ix < size; ix++) {
            if (fields.get(ix).fieldId() == partitionField.fieldId()) {
                break;
            }
        }
        if (ix == size) {
            throw new IllegalStateException("Contract broken");
        }

        // TODO: we may want to have support to widen (or safely tighten) types here, otherwise downstream code could
        // break?
        // For example, DH might want a long partition column, but it's an int here.

        final Object rawValue = data.get(ix);
        if (!partitionField.transform().isIdentity()) {
            return rawValue;
        }

        return IdentityPartitionConverters.convertConstant(data.getType(ix), rawValue);
        // todo: check types?
        /*
         * colValue = IdentityPartitionConverters.convertConstant( partitionData.getType(colData.index),
         * valueFromPartitionData); if (!colData.type.isAssignableFrom(colValue.getClass())) { throw new
         * TableDataException("Partitioning column " + colName + " has type " + colValue.getClass().getName() +
         * " but expected " + colData.type.getName()); }
         */

        // return rawValue;
    }

    @Override
    protected IcebergTableLocationKey keyFromDataFile(
            @NotNull final ManifestFile manifestFile,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        final PartitionData partitionData = (PartitionData) dataFile.partition();
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>(partitionFields.size());
        for (final Map.Entry<String, PartitionField> e : partitionFields.entrySet()) {
            final Object partitionValue = get(e.getValue(), partitionData);
            partitions.put(e.getKey(), (Comparable<?>) partitionValue);
        }
        return locationKey(manifestFile, dataFile, fileUri, partitions, channelsProvider);
    }

    @Override
    public String toString() {
        return IcebergPartitionedLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }
}
