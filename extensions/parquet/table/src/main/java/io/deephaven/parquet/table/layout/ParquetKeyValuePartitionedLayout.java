package io.deephaven.parquet.table.layout;

import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.parquet.table.ParquetTableWriter;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * {@link KeyValuePartitionLayout} for Parquet data.
 */
public class ParquetKeyValuePartitionedLayout extends KeyValuePartitionLayout<ParquetTableLocationKey> {

    public ParquetKeyValuePartitionedLayout(@NotNull final File tableRootDirectory,
            final int maxPartitioningLevels) {
        super(tableRootDirectory,
                path -> path.getFileName().toString().endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION),
                (path, partitions) -> new ParquetTableLocationKey(path.toFile(), 0, partitions),
                maxPartitioningLevels);
    }
}
