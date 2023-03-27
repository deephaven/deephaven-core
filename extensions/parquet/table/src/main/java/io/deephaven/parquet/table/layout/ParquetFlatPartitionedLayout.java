/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.layout;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.deephaven.parquet.table.ParquetTableWriter.PARQUET_FILE_EXTENSION;

/**
 * Parquet {@link TableLocationKeyFinder location finder} that will discover multiple files in a single directory.
 */
public final class ParquetFlatPartitionedLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {
    private static final PathMatcher MATCHER;

    static {
        MATCHER = FileSystems.getDefault().getPathMatcher(String.format("glob:*%s", PARQUET_FILE_EXTENSION));
    }

    private static boolean fileNameMatches(Path path) {
        return MATCHER.matches(path.getFileName());
    }

    private static ParquetTableLocationKey locationKey(Path path) {
        return new ParquetTableLocationKey(path.toFile(), 0, null);
    }

    private final File tableRootDirectory;
    private final Map<Path, ParquetTableLocationKey> cache;

    /**
     * @param tableRootDirectory The directory to search for .parquet files.
     */
    public ParquetFlatPartitionedLayout(@NotNull final File tableRootDirectory) {
        this.tableRootDirectory = tableRootDirectory;
        cache = new ConcurrentHashMap<>();
    }

    public String toString() {
        return ParquetFlatPartitionedLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    @Override
    public void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        try (final DirectoryStream<Path> parquetFileStream = FileSystems.getDefault().provider()
                .newDirectoryStream(tableRootDirectory.toPath(), ParquetFlatPartitionedLayout::fileNameMatches)) {
            for (final Path parquetFilePath : parquetFileStream) {
                locationKeyObserver
                        .accept(cache.computeIfAbsent(parquetFilePath, ParquetFlatPartitionedLayout::locationKey));
            }
        } catch (final IOException e) {
            throw new TableDataException("Error finding parquet locations under " + tableRootDirectory, e);
        }
    }
}
