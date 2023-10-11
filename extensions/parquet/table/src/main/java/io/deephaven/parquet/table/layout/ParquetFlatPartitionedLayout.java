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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Parquet {@link TableLocationKeyFinder location finder} that will discover multiple files in a single directory.
 */
public final class ParquetFlatPartitionedLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {

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
        cache = new HashMap<>();
    }

    public String toString() {
        return ParquetFlatPartitionedLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        try (final DirectoryStream<Path> parquetFileStream =
                Files.newDirectoryStream(tableRootDirectory.toPath(), ParquetFileHelper::parquetFileFilter)) {
            for (final Path parquetFilePath : parquetFileStream) {
                ParquetTableLocationKey locationKey = cache.get(parquetFilePath);
                if (locationKey == null) {
                    locationKey = locationKey(parquetFilePath);
                    if (!locationKey.verifyFileReader()) {
                        continue;
                    }
                    cache.put(parquetFilePath, locationKey);
                }
                locationKeyObserver.accept(locationKey);
            }
        } catch (final IOException e) {
            throw new TableDataException("Error finding parquet locations under " + tableRootDirectory, e);
        }
    }
}
