//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.type.TypeUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables with partitions that will discover data files from
 * a {@link Snapshot}
 */
public final class IcebergKeyValuePartitionedLayout extends IcebergBaseLayout {
    private static class IdentityPartitioningColData {
        final String name;
        final Class<?> type;
        final int index; // position in the partition spec

        private IdentityPartitioningColData(String name, Class<?> type, int index) {
            this.name = name;
            this.type = type;
            this.index = index;
        }
    }

    private final List<IdentityPartitioningColData> identityPartitioningColumns;

    public IcebergKeyValuePartitionedLayout(
            @NotNull IcebergTableAdapter tableAdapter,
            @Nullable Snapshot snapshot,
            @NotNull ParquetInstructions pi,
            @NotNull SeekableChannelsProvider channelsProvider,
            @NotNull PartitionSpec partitionSpec) {
        super(tableAdapter, snapshot, pi, channelsProvider);
        // We can assume due to upstream validation that there are no duplicate names (after renaming) that are included
        // in the output definition, so we can ignore duplicates.
        final List<PartitionField> partitionFields = partitionSpec.fields();
        final int numPartitionFields = partitionFields.size();
        identityPartitioningColumns = new ArrayList<>(numPartitionFields);
        for (int fieldId = 0; fieldId < numPartitionFields; ++fieldId) {
            final PartitionField partitionField = partitionFields.get(fieldId);
            if (!partitionField.transform().isIdentity()) {
                // TODO (DH-18160): Improve support for handling non-identity transforms
                continue;
            }

            // Resolver resolver;

            // resolver.columnInstructions()

            // TODO: use field ids

            throw new RuntimeException("todo");

            // final String icebergColName = partitionField.name();
            // final String dhColName = instructions.columnRenames().getOrDefault(icebergColName, icebergColName);
            // final ColumnDefinition<?> columnDef = tableDef.getColumn(dhColName);
            // if (columnDef == null) {
            // // Table definition provided by the user doesn't have this column, so skip.
            // continue;
            // }
            // identityPartitioningColumns.add(new IdentityPartitioningColData(dhColName,
            // TypeUtils.getBoxedType(columnDef.getDataType()), fieldId));

        }
    }

    @Override
    public String toString() {
        return IcebergKeyValuePartitionedLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(
            @NotNull final ManifestFile manifestFile,
            @NotNull final PartitionSpec spec,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri) {
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
        return locationKey(manifestFile, spec, dataFile, fileUri, partitions);
    }
}
