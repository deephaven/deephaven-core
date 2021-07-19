package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.db.v2.locations.TableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * {@link ParquetTableLocationScanner.LocationKeyFinder Parquet location finder} that will traverse a directory
 * hierarchy and infer partitions from key-value paris in the directory names, e.g.
 * <pre>tableRootDirectory/Country=France/City=Paris/parisData.parquet</pre>.
 * TODO (https://github.com/deephaven/deephaven-core/issues/858): Implement this
 */
public final class HiveStylePartitionLayout implements ParquetTableLocationScanner.LocationKeyFinder {

    private final File tableRootDirectory;
    private boolean inferPartitioningColumnTypes;

    /**
     * @param tableRootDirectory           The directory to traverse from
     * @param inferPartitioningColumnTypes Whether to infer partitioning column types.
     *                                     {@code false} means always use {@code String}.
     */
    public HiveStylePartitionLayout(@NotNull final File tableRootDirectory,
                                    final boolean inferPartitioningColumnTypes) {
        this.tableRootDirectory = tableRootDirectory;
        this.inferPartitioningColumnTypes = inferPartitioningColumnTypes;
    }

    @Override
    public void findKeys(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        throw new UnsupportedOperationException("This is just a placeholder, see: https://github.com/deephaven/deephaven-core/issues/858");
    }
}
