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
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.iceberg.*;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables with partitions that will discover data files from
 * a {@link Snapshot}
 */
public final class IcebergKeyValuePartitionedLayout extends IcebergBaseLayout {
    private static class ColumnData {
        final String name;
        final Class<?> type;
        final int index;

        public ColumnData(String name, Class<?> type, int index) {
            this.name = name;
            this.type = type;
            this.index = index;
        }
    }

    private final List<ColumnData> outputPartitioningColumns;

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
        final MutableInt icebergIndex = new MutableInt(0);
        final Map<String, Integer> availablePartitioningColumns = partitionSpec.fields().stream()
                .map(PartitionField::name)
                .map(name -> instructions.columnRenames().getOrDefault(name, name))
                .collect(Collectors.toMap(
                        name -> name,
                        name -> icebergIndex.getAndIncrement(),
                        (v1, v2) -> v1,
                        LinkedHashMap::new));

        outputPartitioningColumns = tableDef.getColumnStream()
                .map((final ColumnDefinition<?> columnDef) -> {
                    final Integer index = availablePartitioningColumns.get(columnDef.getName());
                    if (index == null) {
                        return null;
                    }
                    return new ColumnData(columnDef.getName(), TypeUtils.getBoxedType(columnDef.getDataType()), index);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
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
        for (final ColumnData colData : outputPartitioningColumns) {
            final String colName = colData.name;
            final Object colValue = partitionData.get(colData.index);
            if (colValue != null && !colData.type.isAssignableFrom(colValue.getClass())) {
                throw new TableDataException("Partitioning column " + colName
                        + " has type " + colValue.getClass().getName()
                        + " but expected " + colData.type.getName());
            }
            partitions.put(colName, (Comparable<?>) colValue);
        }
        return locationKey(manifestFile, dataFile, fileUri, partitions);
    }
}
