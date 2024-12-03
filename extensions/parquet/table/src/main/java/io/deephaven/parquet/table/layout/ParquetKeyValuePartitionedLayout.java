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
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

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

    /**
     * Create a new {@link ParquetKeyValuePartitionedLayout} for the given {@code tableRootDirectory} and
     * {@code tableDefinition}.
     *
     * @param tableRootDirectory The root directory for the table.
     * @param tableDefinition The table definition to use for the layout.
     * @param readInstructions The instructions for customizations while reading.
     * @param channelsProvider The provider for seekable channels. If {@code null}, a new provider will be created and
     *        used for all location keys.
     */
    public static ParquetKeyValuePartitionedLayout create(
            @NotNull final URI tableRootDirectory,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final ParquetInstructions readInstructions,
            @Nullable SeekableChannelsProvider channelsProvider) {
        if (channelsProvider == null) {
            // noinspection resource
            channelsProvider = SeekableChannelsProviderLoader.getInstance()
                    .load(tableRootDirectory.getScheme(), readInstructions.getSpecialInstructions());
        }
        return new ParquetKeyValuePartitionedLayout(tableRootDirectory, tableDefinition, channelsProvider);
    }

    private ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        super(tableRootDirectory,
                () -> new LocationTableBuilderDefinition(tableDefinition),
                (uri, partitions) -> new ParquetTableLocationKey(uri, 0, partitions, channelsProvider),
                Math.toIntExact(tableDefinition.getColumnStream().filter(ColumnDefinition::isPartitioning).count()));
        this.channelsProvider = channelsProvider;
    }

    /**
     * Create a new {@link ParquetKeyValuePartitionedLayout} for the given {@code tableRootDirectory}. The table
     * definition will be inferred from the data using {@link CsvTools#readCsv(java.io.InputStream) CsvTools.readCsv}.
     *
     * @param tableRootDirectory The root directory for the table.
     * @param maxPartitioningLevels The maximum number of partitioning levels to use.
     * @param readInstructions The instructions for customizations while reading.
     * @param channelsProvider The provider for seekable channels. If {@code null}, a new provider will be created and
     *        used for all location keys.
     */

    public static ParquetKeyValuePartitionedLayout create(
            @NotNull final URI tableRootDirectory,
            final int maxPartitioningLevels,
            @NotNull final ParquetInstructions readInstructions,
            @Nullable SeekableChannelsProvider channelsProvider) {
        if (channelsProvider == null) {
            // noinspection resource
            channelsProvider = SeekableChannelsProviderLoader.getInstance()
                    .load(tableRootDirectory.getScheme(), readInstructions.getSpecialInstructions());
        }
        return new ParquetKeyValuePartitionedLayout(tableRootDirectory, maxPartitioningLevels, channelsProvider);
    }

    private ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            final int maxPartitioningLevels,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        super(tableRootDirectory,
                () -> new LocationTableBuilderCsv(tableRootDirectory),
                (uri, partitions) -> new ParquetTableLocationKey(uri, 0, partitions, channelsProvider),
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
