//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.api.util.NameValidator;
import io.deephaven.csv.CsvTools;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.local.LocationTableBuilderDefinition;
import io.deephaven.engine.table.impl.locations.local.URIStreamKeyValuePartitionLayout;
import io.deephaven.engine.table.impl.locations.local.KeyValuePartitionLayout;
import io.deephaven.parquet.base.ParquetUtils;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.deephaven.base.FileUtils.convertToURI;
import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;
import static io.deephaven.parquet.base.ParquetUtils.isVisibleParquetFile;

/**
 * {@link KeyValuePartitionLayout} for Parquet data.
 * 
 * @implNote
 *           <ul>
 *           <li>Unless a {@link TableDefinition} is provided, type inference for partitioning column uses
 *           {@link CsvTools#readCsv(java.io.InputStream) CsvTools.readCsv} as a conversion tool, and hence follows the
 *           same rules.</li>
 *           <li>Column names will be legalized via {@link NameValidator#legalizeColumnName(String, Set)
 *           NameValidator.legalizeColumnName}.</li>
 *           </ul>
 */
public class ParquetKeyValuePartitionedLayout
        extends URIStreamKeyValuePartitionLayout<ParquetTableLocationKey>
        implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private final SeekableChannelsProvider channelsProvider;

    public ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final ParquetInstructions readInstructions) {
        this(tableRootDirectory, tableDefinition, readInstructions,
                SeekableChannelsProviderLoader.getInstance().fromServiceLoader(tableRootDirectory,
                        readInstructions.getSpecialInstructions()));
    }

    private ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        super(tableRootDirectory,
                () -> new LocationTableBuilderDefinition(tableDefinition),
                (uri, partitions) -> new ParquetTableLocationKey(uri, 0, partitions, readInstructions,
                        channelsProvider),
                Math.toIntExact(tableDefinition.getColumnStream().filter(ColumnDefinition::isPartitioning).count()));
        this.channelsProvider = channelsProvider;
    }

    public ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            final int maxPartitioningLevels,
            @NotNull final ParquetInstructions readInstructions) {
        this(tableRootDirectory, maxPartitioningLevels, readInstructions,
                SeekableChannelsProviderLoader.getInstance().fromServiceLoader(tableRootDirectory,
                        readInstructions.getSpecialInstructions()));
    }

    private ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            final int maxPartitioningLevels,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        super(tableRootDirectory,
                () -> new LocationTableBuilderCsv(tableRootDirectory),
                (uri, partitions) -> new ParquetTableLocationKey(uri, 0, partitions, readInstructions,
                        channelsProvider),
                maxPartitioningLevels);
        this.channelsProvider = channelsProvider;
    }

    @Override
    public final void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        final Predicate<URI> uriFilter;
        if (FILE_URI_SCHEME.equals(tableRootDirectory.getScheme())) {
            final Path rootDir = Path.of(tableRootDirectory);
            uriFilter = uri -> isVisibleParquetFile(rootDir, Path.of(uri));
        } else {
            uriFilter = uri -> uri.getPath().endsWith(ParquetUtils.PARQUET_FILE_EXTENSION);
        }
        try (final Stream<URI> filteredUriStream = channelsProvider.walk(tableRootDirectory).filter(uriFilter)) {
            findKeys(filteredUriStream, locationKeyObserver);
        } catch (final IOException e) {
            throw new TableDataException("Error finding parquet locations under " + tableRootDirectory, e);
        }
    }
}
