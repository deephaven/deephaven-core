package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.impl.TableLocationKeyFinder;
import io.deephaven.db.v2.locations.parquet.local.ParquetTableLocationKey;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * {@link TableLocationKeyFinder} that will traverse a directory hierarchy laid out in Deephaven's "nested-partitioned"
 * format, e.g.
 * 
 * <pre>
 * tableRootDirectory/internalPartitionValue/columnPartitionValue/tableName/...
 * </pre>
 * 
 * , producing {@link FileTableLocationKey}'s with two partitions, for keys {@value INTERNAL_PARTITION_KEY} and the
 * specified {@code columnPartitionKey}.
 */
public abstract class DeephavenNestedPartitionLayout<TLK extends FileTableLocationKey>
        implements TableLocationKeyFinder<TLK> {

    @VisibleForTesting
    public static final String PARQUET_FILE_NAME = "table.parquet";

    public static DeephavenNestedPartitionLayout<ParquetTableLocationKey> forParquet(
            @NotNull final File tableRootDirectory,
            @NotNull final String tableName,
            @NotNull final String columnPartitionKey,
            @Nullable final Predicate<String> internalPartitionValueFilter) {
        return new DeephavenNestedPartitionLayout<ParquetTableLocationKey>(tableRootDirectory, tableName,
                columnPartitionKey, internalPartitionValueFilter) {
            @Override
            protected ParquetTableLocationKey makeKey(@NotNull Path tableLeafDirectory,
                    @NotNull Map<String, Comparable<?>> partitions) {
                return new ParquetTableLocationKey(tableLeafDirectory.resolve(PARQUET_FILE_NAME).toFile(), 0,
                        partitions);
            }
        };
    }

    public static final String INTERNAL_PARTITION_KEY = "__INTERNAL_PARTITION__";

    private final File tableRootDirectory;
    private final String tableName;
    private final String columnPartitionKey;
    private final Predicate<String> internalPartitionValueFilter;

    /**
     * @param tableRootDirectory The directory to traverse from
     * @param tableName The table name
     * @param columnPartitionKey The partitioning column name
     * @param internalPartitionValueFilter Filter to control which internal partitions are included, {@code null} for
     *        all
     */
    protected DeephavenNestedPartitionLayout(@NotNull final File tableRootDirectory,
            @NotNull final String tableName,
            @NotNull final String columnPartitionKey,
            @Nullable final Predicate<String> internalPartitionValueFilter) {
        this.tableRootDirectory = tableRootDirectory;
        this.tableName = tableName;
        this.columnPartitionKey = columnPartitionKey;
        this.internalPartitionValueFilter = internalPartitionValueFilter;
    }

    public String toString() {
        return DeephavenNestedPartitionLayout.class.getSimpleName() + '[' + tableRootDirectory + ',' + tableName + ']';
    }

    @Override
    public final void findKeys(@NotNull final Consumer<TLK> locationKeyObserver) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
        PrivilegedFileAccessUtil.doFilesystemAction(() -> {
            try (final DirectoryStream<Path> internalPartitionStream =
                    Files.newDirectoryStream(tableRootDirectory.toPath(), Files::isDirectory)) {
                for (final Path internalPartition : internalPartitionStream) {
                    final String internalPartitionValue = internalPartition.getFileName().toString();
                    if (internalPartitionValueFilter != null
                            && !internalPartitionValueFilter.test(internalPartitionValue)) {
                        continue;
                    }
                    boolean needToUpdateInternalPartitionValue = true;
                    try (final DirectoryStream<Path> columnPartitionStream =
                            Files.newDirectoryStream(internalPartition, Files::isDirectory)) {
                        for (final Path columnPartition : columnPartitionStream) {
                            partitions.put(columnPartitionKey, columnPartition.getFileName().toString());
                            if (needToUpdateInternalPartitionValue) {
                                // Partition order dictates comparison priority, so we need to insert the internal
                                // partition after the column partition.
                                partitions.put(INTERNAL_PARTITION_KEY, internalPartitionValue);
                                needToUpdateInternalPartitionValue = false;
                            }
                            locationKeyObserver.accept(makeKey(columnPartition.resolve(tableName), partitions));
                        }
                    }
                }
            } catch (final NoSuchFileException | FileNotFoundException ignored) {
                // If we found nothing at all, then there's nothing to be done at this level.
            } catch (final IOException e) {
                throw new TableDataException(
                        "Error finding locations for " + tableName + " under " + tableRootDirectory, e);
            }
        });
    }

    protected abstract TLK makeKey(@NotNull final Path tableLeafDirectory,
            @NotNull final Map<String, Comparable<?>> partitions);
}
