//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.parquet.table.ParquetInstructions;
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

    private static ParquetTableLocationKey locationKey(Path path, @NotNull final ParquetInstructions readInstructions) {
        return new ParquetTableLocationKey(path.toFile(), 0, null, readInstructions);
    }

    private final File tableRootDirectory;
    private final Map<Path, ParquetTableLocationKey> cache;
    private final ParquetInstructions readInstructions;

    /**
     * @param tableRootDirectory The directory to search for .parquet files.
     * @param readInstructions the instructions for customizations while reading
     */
    public ParquetFlatPartitionedLayout(@NotNull final File tableRootDirectory,
            @NotNull final ParquetInstructions readInstructions) {
        this.tableRootDirectory = tableRootDirectory;
        this.cache = new HashMap<>();
        this.readInstructions = readInstructions;
    }

    public String toString() {
        return ParquetFlatPartitionedLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        try (final DirectoryStream<Path> parquetFileStream =
                Files.newDirectoryStream(tableRootDirectory.toPath(), ParquetFileHelper::fileNameMatches)) {
            for (final Path parquetFilePath : parquetFileStream) {
                ParquetTableLocationKey locationKey = cache.get(parquetFilePath);
                if (locationKey == null) {
                    locationKey = locationKey(parquetFilePath, readInstructions);
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
