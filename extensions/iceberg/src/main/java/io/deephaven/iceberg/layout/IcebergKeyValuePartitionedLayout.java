//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.util.type.TypeUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.*;

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

    /**
     * @param tableAdapter The {@link IcebergTableAdapter} that will be used to access the table.
     * @param partitionSpec The Iceberg {@link PartitionSpec partition spec} for the table.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergKeyValuePartitionedLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final PartitionSpec partitionSpec,
            @NotNull final IcebergReadInstructions instructions,
            @NotNull final DataInstructionsProviderLoader dataInstructionsProvider) {
        super(tableAdapter, instructions, dataInstructionsProvider);

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
            final String icebergColName = partitionField.name();
            final String dhColName = instructions.columnRenames().getOrDefault(icebergColName, icebergColName);
            final ColumnDefinition<?> columnDef = tableDef.getColumn(dhColName);
            if (columnDef == null) {
                // Table definition provided by the user doesn't have this column, so skip.
                continue;
            }
            identityPartitioningColumns.add(new IdentityPartitioningColData(dhColName,
                    TypeUtils.getBoxedType(columnDef.getDataType()), fieldId));

        }
    }

    @Override
    public String toString() {
        return IcebergKeyValuePartitionedLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(
            @NotNull final ManifestFile manifestFile,
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
        return locationKey(manifestFile, dataFile, fileUri, partitions);
    }
}
