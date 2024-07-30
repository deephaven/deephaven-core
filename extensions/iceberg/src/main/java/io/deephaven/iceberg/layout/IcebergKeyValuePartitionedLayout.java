//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergInstructions;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
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
     * @param tableDef The {@link TableDefinition} that will be used for the table.
     * @param table The {@link Table} to discover locations for.
     * @param tableSnapshot The {@link Snapshot} from which to discover data files.
     * @param fileIO The file IO to use for reading manifest data files.
     * @param partitionSpec The Iceberg {@link PartitionSpec partition spec} for the table.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergKeyValuePartitionedLayout(
            @NotNull final TableDefinition tableDef,
            @NotNull final org.apache.iceberg.Table table,
            @NotNull final org.apache.iceberg.Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final PartitionSpec partitionSpec,
            @NotNull final IcebergInstructions instructions,
            @NotNull final Map<String, String> properties) {
        super(tableDef, table, tableSnapshot, fileIO, instructions, properties);

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
        return IcebergKeyValuePartitionedLayout.class.getSimpleName() + '[' + table.name() + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(DataFile df, URI fileUri) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();

        final PartitionData partitionData = (PartitionData) df.partition();
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
        return locationKey(df.format(), fileUri, partitions);
    }
}
