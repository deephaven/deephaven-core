//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.parquet.base.ParquetUtils;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;

/**
 * Parquet {@link TableLocationKeyFinder location finder} that will discover multiple files in a single directory.
 */
public final class ParquetFlatPartitionedLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private static ParquetTableLocationKey locationKey(@NotNull final URI uri,
            @NotNull final ParquetInstructions readInstructions) {
        return new ParquetTableLocationKey(uri, 0, null, readInstructions);
    }

    private final URI tableRootDirectory;
    private final Map<URI, ParquetTableLocationKey> cache;
    private final ParquetInstructions readInstructions;

    /**
     * @param tableRootDirectory The directory to search for .parquet files.
     * @param readInstructions the instructions for customizations while reading
     */
    public ParquetFlatPartitionedLayout(@NotNull final File tableRootDirectory,
            @NotNull final ParquetInstructions readInstructions) {
        this(FileUtils.convertToURI(tableRootDirectory, true), readInstructions);
    }

    /**
     * @param tableRootDirectoryURI The directory URI to search for .parquet files.
     * @param readInstructions the instructions for customizations while reading
     */
    public ParquetFlatPartitionedLayout(@NotNull final URI tableRootDirectoryURI,
            @NotNull final ParquetInstructions readInstructions) {
        this.tableRootDirectory = tableRootDirectoryURI;
        this.cache = Collections.synchronizedMap(new HashMap<>());
        this.readInstructions = readInstructions;
    }

    public String toString() {
        return ParquetFlatPartitionedLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    @Override
    public void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        final Predicate<URI> uriFilter;
        if (FILE_URI_SCHEME.equals(tableRootDirectory.getScheme())) {
            uriFilter = uri -> {
                final String filename = new File(uri).getName();
                return filename.endsWith(ParquetUtils.PARQUET_FILE_EXTENSION) && filename.charAt(0) != '.';
            };
        } else {
            uriFilter = uri -> uri.getPath().endsWith(ParquetUtils.PARQUET_FILE_EXTENSION);
        }
        try (final SeekableChannelsProvider provider = SeekableChannelsProviderLoader.getInstance().fromServiceLoader(
                tableRootDirectory, readInstructions.getSpecialInstructions());
                final Stream<URI> stream = provider.list(tableRootDirectory)) {
            stream.filter(uriFilter).forEach(uri -> {
                cache.compute(uri, (key, existingLocationKey) -> {
                    if (existingLocationKey != null) {
                        locationKeyObserver.accept(existingLocationKey);
                        return existingLocationKey;
                    }
                    final ParquetTableLocationKey newLocationKey = locationKey(uri, readInstructions);
                    if (!newLocationKey.verifyFileReader()) {
                        return null;
                    }
                    locationKeyObserver.accept(newLocationKey);
                    return newLocationKey;
                });
            });
        } catch (final IOException e) {
            throw new TableDataException("Error finding parquet locations under " + tableRootDirectory, e);
        }
    }
}
