//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.csv.CsvTools;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.local.KeyValuePartitionLayout;
import io.deephaven.engine.table.impl.locations.local.LocationTableBuilderDefinition;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * {@link KeyValuePartitionLayout} for Parquet data.
 * 
 * @implNote Type inference uses {@link CsvTools#readCsv(java.io.InputStream)} as a conversion tool, and hence follows
 *           the same rules.
 */
public class ParquetKeyValuePartitionedLayout extends KeyValuePartitionLayout<ParquetTableLocationKey> {

    public ParquetKeyValuePartitionedLayout(
            @NotNull final File tableRootDirectory,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableRootDirectory,
                ParquetFileHelper::fileNameMatches,
                () -> new LocationTableBuilderDefinition(tableDefinition),
                (path, partitions) -> new ParquetTableLocationKey(path.toFile(), 0, partitions, readInstructions),
                Math.toIntExact(tableDefinition.getColumnStream().filter(ColumnDefinition::isPartitioning).count()));
    }

    public ParquetKeyValuePartitionedLayout(
            @NotNull final File tableRootDirectory,
            final int maxPartitioningLevels,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableRootDirectory,
                ParquetFileHelper::fileNameMatches,
                () -> new LocationTableBuilderCsv(tableRootDirectory),
                (path, partitions) -> new ParquetTableLocationKey(path.toFile(), 0, partitions, readInstructions),
                maxPartitioningLevels);
    }
}
