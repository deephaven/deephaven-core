//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.stats.Counter;
import io.deephaven.base.stats.State;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.Value;
import io.deephaven.csv.CsvTools;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.local.LocationTableBuilderDefinition;
import io.deephaven.engine.table.impl.locations.local.URIStreamKeyValuePartitionLayout;
import io.deephaven.engine.table.impl.locations.local.KeyValuePartitionLayout;
import io.deephaven.engine.readtracker.impl.QueryPerformanceReadTracker;
import io.deephaven.parquet.base.ParquetUtils;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetSchemaReader;
import io.deephaven.parquet.table.metadata.PartitioningColumnInfo;
import io.deephaven.parquet.table.metadata.TableInfo;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.deephaven.base.FileUtils.URI_SEPARATOR;
import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;
import static io.deephaven.parquet.base.ParquetUtils.isVisibleParquetFile;

/**
 * {@link KeyValuePartitionLayout} for Parquet data.
 * 
 * @implNote
 *           <ul>
 *           <li>When no {@link TableDefinition} is provided, the partitioning columns' types come from the
 *           {@link TableInfo#partitioningColumns() partitioning columns} recorded in the first file's Deephaven
 *           metadata, so a Deephaven-written dataset round trips exactly.</li>
 *           <li>Failing that -- a dataset Deephaven did not write, or one written before that field existed -- type
 *           inference uses {@link CsvTools#readCsv(java.io.InputStream) CsvTools.readCsv} as a conversion tool, and
 *           hence follows the same rules. Note that inference is text-directed and does not necessarily recover the
 *           type a column was written with.</li>
 *           <li>Column names will be legalized via {@link NameValidator#legalizeColumnName(String, Set)
 *           NameValidator.legalizeColumnName}.</li>
 *           </ul>
 */
public class ParquetKeyValuePartitionedLayout
        extends URIStreamKeyValuePartitionLayout<ParquetTableLocationKey>
        implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private final SeekableChannelsProvider channelsProvider;

    /**
     * {@code true} when this layout was created without a {@link TableDefinition} and must therefore determine the
     * partitioning columns' types itself.
     */
    private final boolean inferring;

    private static final Value WALK_DURATION_NANOS =
            Stats.makeItem("ParquetKeyValuePartitionedLayout", "walk", Counter.FACTORY).getValue();

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
        this.inferring = false;
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
        this.inferring = true;
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
        final long start = System.nanoTime();
        try (final Stream<URI> filteredUriStream = channelsProvider.walk(tableRootDirectory).filter(uriFilter)) {
            if (inferring) {
                findKeysInferringPartitionTypes(filteredUriStream, locationKeyObserver);
            } else {
                findKeys(filteredUriStream, locationKeyObserver);
            }
        } catch (final IOException e) {
            throw new TableDataException("Error finding parquet locations under " + tableRootDirectory, e);
        } finally {
            final long duration = System.nanoTime() - start;
            WALK_DURATION_NANOS.sample(duration);
            QueryPerformanceReadTracker.recordMetadataOperation(duration);
        }
    }

    /**
     * Traverse {@code uriStream} without a caller-supplied {@link TableDefinition}, preferring the partitioning
     * columns' recorded types over inferring them from the directory names.
     *
     * <p>
     * A Deephaven-written key-value partitioned file records its partitioning columns and their types in its
     * {@link TableInfo}, because those values live in the directory path rather than in any file's parquet schema.
     * Inference from the key text cannot recover them: a {@code String} column whose values are all one character reads
     * back as {@code char}, one whose values look like integers reads back as {@code int}, and a value such as
     * {@code "01"} then reads back as {@code 1} -- which no longer reproduces its own directory name.
     *
     * <p>
     * Only the first file is consulted, and only its metadata; the stream is otherwise passed through untouched, so
     * this costs one footer read. Falls back to {@link LocationTableBuilderCsv} inference when the file records no
     * partitioning columns -- an empty stream, a file written before the field existed, a dataset not written by
     * Deephaven -- or when the recorded names do not match the directories actually found.
     */
    private void findKeysInferringPartitionTypes(
            @NotNull final Stream<URI> uriStream,
            @NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        final Iterator<URI> uriIterator = uriStream.iterator();
        if (!uriIterator.hasNext()) {
            findKeys(Stream.empty(), new LocationTableBuilderCsv(tableRootDirectory), locationKeyObserver);
            return;
        }
        final URI firstUri = uriIterator.next();
        final TableDefinition recordedDefinition = recordedPartitioningDefinition(firstUri);
        final KeyValuePartitionLayout.LocationTableBuilder locationTableBuilder =
                recordedDefinition != null && describesPartitionsOf(recordedDefinition, firstUri)
                        ? new LocationTableBuilderDefinition(recordedDefinition)
                        : new LocationTableBuilderCsv(tableRootDirectory);
        // Rebuild the stream rather than collecting it: the base traversal already queues every URI, and a large
        // dataset should not pay for a second copy.
        final Stream<URI> rebuiltStream = Stream.concat(Stream.of(firstUri),
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(uriIterator, Spliterator.ORDERED), false));
        findKeys(rebuiltStream, locationTableBuilder, locationKeyObserver);
    }

    /**
     * Check that {@code recordedDefinition} names exactly the partitioning columns that {@code uri}'s own directories
     * do.
     *
     * <p>
     * {@link LocationTableBuilderDefinition} requires an exact match and throws otherwise, which would turn a dataset
     * whose directories were reorganized after writing into a hard read failure. Checking here instead keeps inference
     * available as the fallback, and costs only a look at the path we have already resolved.
     */
    private boolean describesPartitionsOf(
            @NotNull final TableDefinition recordedDefinition,
            @NotNull final URI uri) {
        final String relativePath = tableRootDirectory.relativize(uri).getPath();
        final String[] subDirs = relativePath.split(URI_SEPARATOR);
        final Set<String> pathKeys = new HashSet<>(subDirs.length);
        for (int ii = 0; ii < subDirs.length - 1; ++ii) {
            final int equalsIndex = subDirs[ii].indexOf('=');
            if (equalsIndex <= 0) {
                return false;
            }
            pathKeys.add(NameValidator.legalizeColumnName(subDirs[ii].substring(0, equalsIndex), Set.of()));
        }
        return pathKeys.equals(new HashSet<>(recordedDefinition.getColumnNames()));
    }

    /**
     * Read the partitioning columns recorded in {@code uri}'s Deephaven metadata.
     *
     * @return A {@link TableDefinition} of partitioning columns, or {@code null} if the file records none or records
     *         one whose type partition values cannot be parsed to
     */
    @Nullable
    private TableDefinition recordedPartitioningDefinition(@NotNull final URI uri) {
        final List<PartitioningColumnInfo> partitioningColumns;
        try {
            final ParquetTableLocationKey firstKey = new ParquetTableLocationKey(uri, 0, null, channelsProvider);
            partitioningColumns = ParquetSchemaReader
                    .parseMetadata(firstKey.getMetadata().getFileMetaData().getKeyValueMetaData())
                    .map(TableInfo::partitioningColumns)
                    .orElse(List.of());
        } catch (final RuntimeException e) {
            // Reading the footer is best-effort here; a genuinely unreadable file will fail again, with a better
            // message, once it is opened as a location.
            return null;
        }
        if (partitioningColumns.isEmpty()) {
            return null;
        }
        final List<ColumnDefinition<?>> columnDefinitions = new ArrayList<>(partitioningColumns.size());
        for (final PartitioningColumnInfo partitioningColumn : partitioningColumns) {
            final ColumnDefinition<?> columnDefinition = partitioningColumn.columnDefinition();
            if (columnDefinition == null) {
                // A type with no partition-value parser; inference is all that is left.
                return null;
            }
            columnDefinitions.add(columnDefinition);
        }
        return TableDefinition.of(columnDefinitions);
    }
}
