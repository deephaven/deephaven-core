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
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.*;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables with partitions that will discover data files from
 * a {@link Snapshot}
 */
public final class IcebergKeyValuePartitionedLayout extends IcebergBaseLayout {
    private final String[] inputPartitionColumnNames;

    private class ColumnData {
        final String name;
        final Class<?> type;
        final int index;

        public ColumnData(String name, Class<?> type, int index) {
            this.name = name;
            this.type = type;
            this.index = index;
        }
    }

    private final List<ColumnData> outputPartitionColumns;

    /**
     * @param tableDef The {@link TableDefinition} that will be used for the table.
     * @param table The {@link Table} to discover locations for.
     * @param tableSnapshot The {@link Snapshot} from which to discover data files.
     * @param fileIO The file IO to use for reading manifest data files.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergKeyValuePartitionedLayout(
            @NotNull final TableDefinition tableDef,
            @NotNull final org.apache.iceberg.Table table,
            @NotNull final org.apache.iceberg.Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final PartitionSpec partitionSpec,
            @NotNull final IcebergInstructions instructions) {
        super(tableDef, table, tableSnapshot, fileIO, instructions);

        // Get the list of (potentially renamed) columns on which the Iceberg table is partitioned. This will be the
        // order of the values in DataFile.partition() collection.
        inputPartitionColumnNames = partitionSpec.fields().stream()
                .map(PartitionField::name)
                .map(col -> instructions.columnRenameMap().getOrDefault(col, col))
                .toArray(String[]::new);

        outputPartitionColumns = new ArrayList<>();

        // Get the list of columns in the table definition that are included in the partition columns.
        final List<String> outputCols = tableDef.getColumnNames();
        outputCols.retainAll(List.of(inputPartitionColumnNames));

        for (String col : outputCols) {
            // Is this so inefficient that it's worth it to use a map?
            for (int i = 0; i < inputPartitionColumnNames.length; i++) {
                if (inputPartitionColumnNames[i].equals(col)) {
                    outputPartitionColumns
                            .add(new ColumnData(col, TypeUtils.getBoxedType(tableDef.getColumn(col).getDataType()), i));
                    break;
                }
            }
        }
    }

    @Override
    public String toString() {
        return IcebergKeyValuePartitionedLayout.class.getSimpleName() + '[' + table.name() + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(DataFile df, URI fileUri) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();

        final PartitionData partitionData = (PartitionData) df.partition();
        for (ColumnData colData : outputPartitionColumns) {
            final String colName = colData.name;
            final Object value = partitionData.get(colData.index);
            if (value != null && !value.getClass().isAssignableFrom(colData.type)) {
                throw new TableDataException("Partitioning column " + colName
                        + " has type " + value.getClass().getName()
                        + " but expected " + colData.type.getName());
            }
            partitions.put(colName, (Comparable<?>) value);
        }
        return locationKey(df.format(), fileUri, partitions);
    }
}
