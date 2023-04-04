/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.layout;

import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * {@link KeyValuePartitionLayout} for Parquet data.
 */
public class ParquetKeyValuePartitionedLayout extends KeyValuePartitionLayout<ParquetTableLocationKey> {

    public ParquetKeyValuePartitionedLayout(@NotNull final File tableRootDirectory,
            final int maxPartitioningLevels) {
        super(tableRootDirectory,
                ParquetFileHelper::fileNameMatches,
                (path, partitions) -> new ParquetTableLocationKey(path.toFile(), 0, partitions),
                maxPartitioningLevels);
    }
}
