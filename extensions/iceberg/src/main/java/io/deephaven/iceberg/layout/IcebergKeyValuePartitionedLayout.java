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
    private final String[] partitionColumns;
    private final Class<?>[] partitionColumnTypes;

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
            @NotNull final IcebergInstructions instructions) {
        super(tableDef, table, tableSnapshot, fileIO, instructions);

        partitionColumns =
                tableDef.getPartitioningColumns().stream().map(ColumnDefinition::getName).toArray(String[]::new);
        partitionColumnTypes = Arrays.stream(partitionColumns)
                .map(colName -> TypeUtils.getBoxedType(tableDef.getColumn(colName).getDataType()))
                .toArray(Class<?>[]::new);
    }

    @Override
    public String toString() {
        return IcebergKeyValuePartitionedLayout.class.getSimpleName() + '[' + table.name() + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(DataFile df, URI fileUri) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();

        final PartitionData partitionData = (PartitionData) df.partition();
        for (int ii = 0; ii < partitionColumns.length; ++ii) {
            final Object value = partitionData.get(ii);
            if (value != null && !value.getClass().isAssignableFrom(partitionColumnTypes[ii])) {
                throw new TableDataException("Partitioning column " + partitionColumns[ii]
                        + " has type " + value.getClass().getName()
                        + " but expected " + partitionColumnTypes[ii].getName());
            }
            partitions.put(partitionColumns[ii], (Comparable<?>) value);
        }
        return locationKey(df.format(), fileUri, partitions);
    }
}
