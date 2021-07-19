package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.local.PrivilegedFileAccessUtil;
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
 * {@link ParquetTableLocationScanner.LocationKeyFinder Parquet location finder} that will traverse a directory
 * hierarchy laid out in Deephaven's "nested-partitioned" format, e.g.
 * <pre>tableRootDirectory/internalPartitionValue/columnPartitionValue/tableName/table.parquet</pre>, producing
 * {@link ParquetTableLocationKey}'s with two partitions, for keys {@value INTERNAL_PARTITION_KEY} and the specified
 * {@code columnPartitionKey}.
 */
public final class DeephavenStylePartitionLayout implements ParquetTableLocationScanner.LocationKeyFinder {

    @VisibleForTesting
    public static final String PARQUET_FILE_NAME = "table.parquet";
    public static final String INTERNAL_PARTITION_KEY = "__INTERNAL_PARTITION__";

    private final File tableRootDirectory;
    private final String tableName;
    private final String columnPartitionKey;
    private final Predicate<String> internalPartitionValueFilter;

    /**
     * @param tableRootDirectory           The directory to traverse from
     * @param tableName                    The table name
     * @param columnPartitionKey           The partitioning column name
     * @param internalPartitionValueFilter Filter to control which internal partitions are included, {@code null} for all
     */
    public DeephavenStylePartitionLayout(@NotNull final File tableRootDirectory,
                                         @NotNull final String tableName,
                                         @NotNull final String columnPartitionKey,
                                         @Nullable final Predicate<String> internalPartitionValueFilter) {
        this.tableRootDirectory = tableRootDirectory;
        this.tableName = tableName;
        this.columnPartitionKey = columnPartitionKey;
        this.internalPartitionValueFilter = internalPartitionValueFilter;
    }

    @Override
    public void findKeys(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
        PrivilegedFileAccessUtil.doFilesystemAction(() -> {
            try (final DirectoryStream<Path> internalPartitionStream = Files.newDirectoryStream(tableRootDirectory.toPath(), Files::isDirectory)) {
                for (final Path internalPartition : internalPartitionStream) {
                    final String internalPartitionValue = internalPartition.getFileName().toString();
                    if (internalPartitionValueFilter != null && !internalPartitionValueFilter.test(internalPartitionValue)) {
                        continue;
                    }
                    boolean needToUpdateInternalPartitionValue = true;
                    try (final DirectoryStream<Path> columnPartitionStream = Files.newDirectoryStream(internalPartition, Files::isDirectory)) {
                        for (final Path columnPartition : columnPartitionStream) {
                            partitions.put(columnPartitionKey, columnPartition.getFileName().toFile());
                            if (needToUpdateInternalPartitionValue) {
                                // Partition order dictates comparison priority, so we need to insert the internal partition after the column partition.
                                partitions.put(INTERNAL_PARTITION_KEY, internalPartitionValue);
                                needToUpdateInternalPartitionValue = false;
                            }
                            locationKeyObserver.accept(new ParquetTableLocationKey(columnPartition.resolve(tableName).resolve(PARQUET_FILE_NAME).toFile(), partitions));
                        }
                    }
                }
            } catch (final NoSuchFileException | FileNotFoundException ignored) {
                // If we found nothing at all, then there's nothing to be done at this level.
            } catch (final IOException e) {
                throw new TableDataException("DeephavenStylePartitionLayout-" + tableName + ": Error finding locations under " + tableRootDirectory, e);
            }
        });
    }
}
