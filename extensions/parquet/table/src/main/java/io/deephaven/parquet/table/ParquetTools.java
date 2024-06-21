//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.FileUtils;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.PartitionedTableFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.util.PartitionFormatter;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.parquet.base.ParquetMetadataFileWriter;
import io.deephaven.parquet.base.NullParquetMetadataFileWriter;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.channel.SeekableChannelsProviderPlugin;
import io.deephaven.vector.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.SimpleSourceTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.KnownLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.impl.PollingTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.parquet.base.ParquetFileReader;
import io.deephaven.parquet.table.layout.ParquetFlatPartitionedLayout;
import io.deephaven.parquet.table.layout.ParquetKeyValuePartitionedLayout;
import io.deephaven.parquet.table.layout.ParquetMetadataFileLayout;
import io.deephaven.parquet.table.location.ParquetTableLocationFactory;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.schema.MessageType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.deephaven.base.FileUtils.convertToURI;
import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;
import static io.deephaven.parquet.base.ParquetUtils.COMMON_METADATA_FILE_URI_SUFFIX;
import static io.deephaven.parquet.base.ParquetUtils.METADATA_FILE_URI_SUFFIX;
import static io.deephaven.parquet.base.ParquetUtils.isParquetFile;
import static io.deephaven.parquet.table.ParquetInstructions.FILE_INDEX_TOKEN;
import static io.deephaven.parquet.table.ParquetInstructions.PARTITIONS_TOKEN;
import static io.deephaven.parquet.table.ParquetInstructions.UUID_TOKEN;
import static io.deephaven.parquet.base.ParquetUtils.PARQUET_FILE_EXTENSION;
import static io.deephaven.parquet.base.ParquetUtils.COMMON_METADATA_FILE_NAME;
import static io.deephaven.parquet.base.ParquetUtils.METADATA_FILE_NAME;
import static io.deephaven.parquet.table.ParquetTableWriter.getSchemaForTable;
import static io.deephaven.util.type.TypeUtils.getUnboxedTypeIfBoxed;

/**
 * Tools for managing and manipulating tables on disk in parquet format.
 */
@SuppressWarnings("WeakerAccess")
public class ParquetTools {

    private static final int MAX_PARTITIONING_LEVELS_INFERENCE = 32;
    private static final Collection<List<String>> EMPTY_INDEXES = Collections.emptyList();

    private ParquetTools() {}

    private static final Logger log = LoggerFactory.getLogger(ParquetTools.class);

    /**
     * Reads in a table from a single parquet file, metadata file, or directory with recognized layout. The source
     * provided can be a local file path or a URI to be resolved via the provided
     * {@link SeekableChannelsProviderPlugin}.
     *
     * <p>
     * This method attempts to "do the right thing." It examines the source to determine if it's a single parquet file,
     * a metadata file, or a directory. If it's a directory, it additionally tries to guess the layout to use. Unless a
     * metadata file is supplied or discovered in the directory, the highest (by {@link ParquetTableLocationKey location
     * key} order) location found will be used to infer schema.
     *
     * @param source The path or URI of file or directory to examine
     * @return table
     * @see ParquetMetadataFileLayout
     * @see ParquetKeyValuePartitionedLayout
     * @see ParquetFlatPartitionedLayout
     */
    public static Table readTable(@NotNull final String source) {
        return readTable(source, ParquetInstructions.EMPTY);
    }

    /**
     * Reads in a table from a single parquet file, metadata file, or directory with recognized layout. The source
     * provided can be a local file path or a URI to be resolved via the provided
     * {@link SeekableChannelsProviderPlugin}.
     *
     * <p>
     * If the {@link ParquetFileLayout} is not provided in the {@link ParquetInstructions instructions}, this method
     * attempts to "do the right thing." It examines the source to determine if it's a single parquet file, a metadata
     * file, or a directory. If it's a directory, it additionally tries to guess the layout to use. Unless a metadata
     * file is supplied or discovered in the directory, the highest (by {@link ParquetTableLocationKey location key}
     * order) location found will be used to infer schema.
     *
     * @param source The path or URI of file or directory to examine
     * @param readInstructions Instructions for customizations while reading
     * @return table
     * @see ParquetMetadataFileLayout
     * @see ParquetKeyValuePartitionedLayout
     * @see ParquetFlatPartitionedLayout
     */
    public static Table readTable(
            @NotNull final String source,
            @NotNull final ParquetInstructions readInstructions) {
        final boolean isDirectory = !isParquetFile(source);
        final URI sourceURI = convertToURI(source, isDirectory);
        if (readInstructions.getFileLayout().isPresent()) {
            switch (readInstructions.getFileLayout().get()) {
                case SINGLE_FILE:
                    return readSingleFileTable(sourceURI, readInstructions);
                case FLAT_PARTITIONED:
                    return readFlatPartitionedTable(sourceURI, readInstructions);
                case KV_PARTITIONED:
                    return readKeyValuePartitionedTable(sourceURI, readInstructions);
                case METADATA_PARTITIONED:
                    return readPartitionedTableWithMetadata(sourceURI, readInstructions);
            }
        }
        if (FILE_URI_SCHEME.equals(sourceURI.getScheme())) {
            return readTableFromFileUri(sourceURI, readInstructions);
        }
        if (source.endsWith(METADATA_FILE_URI_SUFFIX) || source.endsWith(COMMON_METADATA_FILE_URI_SUFFIX)) {
            throw new UnsupportedOperationException("We currently do not support reading parquet metadata files " +
                    "from non local storage");
        }
        if (!isDirectory) {
            return readSingleFileTable(sourceURI, readInstructions);
        }
        // Both flat partitioned and key-value partitioned data can be read under key-value partitioned layout
        return readKeyValuePartitionedTable(sourceURI, readInstructions);
    }

    /**
     * Write a table to a file. Data indexes to write are determined by those present on {@code sourceTable}.
     *
     * @param sourceTable source table
     * @param destination destination path or URI; the file name should end in ".parquet" extension. If the path
     *        includes non-existing directories, they are created. If there is an error any intermediate directories
     *        previously created are removed; note this makes this method unsafe for concurrent use
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final String destination) {
        writeTables(new Table[] {sourceTable}, new String[] {destination},
                ParquetInstructions.EMPTY.withTableDefinition(sourceTable.getDefinition()));
    }

    /**
     * Write a table to a file. Data indexes to write are determined by those present on {@code sourceTable}.
     *
     * @param sourceTable source table
     * @param destination destination path or URI; the file name should end in ".parquet" extension. If the path
     *        includes non-existing directories, they are created. If there is an error any intermediate directories
     *        previously created are removed; note this makes this method unsafe for concurrent use
     * @param writeInstructions instructions for customizations while writing
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final String destination,
            @NotNull final ParquetInstructions writeInstructions) {
        writeTables(new Table[] {sourceTable}, new String[] {destination},
                ensureTableDefinition(writeInstructions, sourceTable.getDefinition(), false));
    }

    private static ParquetInstructions ensureTableDefinition(
            @NotNull final ParquetInstructions instructions,
            @NotNull final TableDefinition definition,
            final boolean validateExisting) {
        if (instructions.getTableDefinition().isEmpty()) {
            return instructions.withTableDefinition(definition);
        } else if (validateExisting && !instructions.getTableDefinition().get().equals(definition)) {
            throw new IllegalArgumentException(
                    "Table definition provided in instructions does not match the one provided in the method call");
        }
        return instructions;
    }

    private static File getShadowFile(final File destFile) {
        return new File(destFile.getParent(), ".NEW_" + destFile.getName());
    }

    @VisibleForTesting
    static File getBackupFile(final File destFile) {
        return new File(destFile.getParent(), ".OLD_" + destFile.getName());
    }

    private static String minusParquetSuffix(@NotNull final String s) {
        if (s.endsWith(PARQUET_FILE_EXTENSION)) {
            return s.substring(0, s.length() - PARQUET_FILE_EXTENSION.length());
        }
        return s;
    }

    /**
     * Generates the index file path relative to the table destination file path.
     *
     * @param tableDest Destination path for the main table containing these indexing columns
     * @param columnNames Array of indexing column names
     *
     * @return The relative index file path. For example, for table with destination {@code "table.parquet"} and
     *         indexing column {@code "IndexingColName"}, the method will return
     *         {@code ".dh_metadata/indexes/IndexingColName/index_IndexingColName_table.parquet"} on unix systems.
     */
    @VisibleForTesting
    static String getRelativeIndexFilePath(@NotNull final File tableDest, @NotNull final String... columnNames) {
        final String columns = String.join(",", columnNames);
        return String.format(".dh_metadata%sindexes%s%s%sindex_%s_%s", File.separator, File.separator, columns,
                File.separator, columns, tableDest.getName());
    }

    /**
     * Legacy method for generating a grouping file name. We used to place grouping files right next to the original
     * table destination.
     *
     * @param tableDest Destination path for the main table containing these grouping columns
     * @param columnName Name of the grouping column
     *
     * @return The relative grouping file path. For example, for table with destination {@code "table.parquet"} and
     *         grouping column {@code "GroupingColName"}, the method will return
     *         {@code "table_GroupingColName_grouping.parquet"}
     */
    @VisibleForTesting
    public static String legacyGroupingFileName(@NotNull final File tableDest, @NotNull final String columnName) {
        final String prefix = minusParquetSuffix(tableDest.getName());
        return prefix + "_" + columnName + "_grouping.parquet";
    }

    /**
     * Delete any old backup files created for this destination, and throw an exception on failure
     */
    private static void deleteBackupFile(@NotNull final File destFile) {
        if (!deleteBackupFileNoExcept(destFile)) {
            throw new UncheckedDeephavenException(
                    String.format("Failed to delete backup file at %s", getBackupFile(destFile).getAbsolutePath()));
        }
    }

    /**
     * Delete any old backup files created for this destination with no exception in case of failure
     */
    private static boolean deleteBackupFileNoExcept(@NotNull final File destFile) {
        final File backupDestFile = getBackupFile(destFile);
        if (backupDestFile.exists() && !backupDestFile.delete()) {
            log.error().append("Error in deleting backup file at path ")
                    .append(backupDestFile.getAbsolutePath())
                    .endl();
            return false;
        }
        return true;
    }

    /**
     * Backup any existing files at location destFile and rename the shadow file to destFile
     */
    private static void installShadowFile(@NotNull final File destFile, @NotNull final File shadowDestFile) {
        final File backupDestFile = getBackupFile(destFile);
        if (destFile.exists() && !destFile.renameTo(backupDestFile)) {
            throw new UncheckedDeephavenException(
                    String.format(
                            "Failed to install shadow file at %s because a file already exists at the path which couldn't be renamed to %s",
                            destFile.getAbsolutePath(), backupDestFile.getAbsolutePath()));
        }
        if (!shadowDestFile.renameTo(destFile)) {
            throw new UncheckedDeephavenException(String.format(
                    "Failed to install shadow file at %s because couldn't rename temporary shadow file from %s to %s",
                    destFile.getAbsolutePath(), shadowDestFile.getAbsolutePath(), destFile.getAbsolutePath()));
        }
    }

    /**
     * Roll back any changes made in the {@link #installShadowFile} in best-effort manner
     */
    private static void rollbackFile(@NotNull final File destFile) {
        final File backupDestFile = getBackupFile(destFile);
        final File shadowDestFile = getShadowFile(destFile);
        destFile.renameTo(shadowDestFile);
        backupDestFile.renameTo(destFile);
    }

    /**
     * Make any missing ancestor directories of {@code destination}.
     *
     * @param destination The destination parquet file
     * @return The first created directory, or null if no directories were made.
     */
    private static File prepareDestinationFileLocation(@NotNull File destination) {
        destination = destination.getAbsoluteFile();
        if (!destination.getPath().endsWith(PARQUET_FILE_EXTENSION)) {
            throw new UncheckedDeephavenException(
                    String.format("Destination %s does not end in %s extension", destination, PARQUET_FILE_EXTENSION));
        }
        if (destination.exists()) {
            if (destination.isDirectory()) {
                throw new UncheckedDeephavenException(
                        String.format("Destination %s exists and is a directory", destination));
            }
            if (!destination.canWrite()) {
                throw new UncheckedDeephavenException(
                        String.format("Destination %s exists but is not writable", destination));
            }
            return null;
        }
        final File firstParent = destination.getParentFile();
        if (firstParent.isDirectory()) {
            if (firstParent.canWrite()) {
                return null;
            }
            throw new UncheckedDeephavenException(
                    String.format("Destination %s has non writable parent directory", destination));
        }
        File firstCreated = firstParent;
        File parent;
        for (parent = destination.getParentFile(); parent != null && !parent.exists(); parent =
                parent.getParentFile()) {
            firstCreated = parent;
        }
        if (parent == null) {
            throw new IllegalArgumentException(
                    String.format("Can't find any existing parent directory for destination path: %s", destination));
        }
        if (!parent.isDirectory()) {
            throw new IllegalArgumentException(
                    String.format("Existing parent file %s of %s is not a directory", parent, destination));
        }
        if (!firstParent.mkdirs()) {
            throw new UncheckedDeephavenException("Couldn't (re)create destination directory " + firstParent);
        }
        return firstCreated;
    }

    /**
     * Helper function for building index column info for writing and deleting any backup index column files
     *
     * @param indexColumns Names of index columns, stored as String list for each index
     * @param parquetColumnNameArr Names of index columns for the parquet file, stored as String[] for each index
     * @param destFile The destination path for the main table containing these index columns
     */
    private static List<ParquetTableWriter.IndexWritingInfo> indexInfoBuilderHelper(
            @NotNull final Collection<List<String>> indexColumns,
            @NotNull final String[][] parquetColumnNameArr,
            @NotNull final File destFile) {
        Require.eq(indexColumns.size(), "indexColumns.size", parquetColumnNameArr.length,
                "parquetColumnNameArr.length");
        final int numIndexes = indexColumns.size();
        final List<ParquetTableWriter.IndexWritingInfo> indexInfoList = new ArrayList<>(numIndexes);
        int gci = 0;
        for (final List<String> indexColumnNames : indexColumns) {
            final String[] parquetColumnNames = parquetColumnNameArr[gci];
            final String indexFileRelativePath = getRelativeIndexFilePath(destFile, parquetColumnNames);
            final File indexFile = new File(destFile.getParent(), indexFileRelativePath);
            prepareDestinationFileLocation(indexFile);
            deleteBackupFile(indexFile);

            final File shadowIndexFile = getShadowFile(indexFile);

            final ParquetTableWriter.IndexWritingInfo info = new ParquetTableWriter.IndexWritingInfo(
                    indexColumnNames,
                    parquetColumnNames,
                    indexFile,
                    shadowIndexFile);
            indexInfoList.add(info);
            gci++;
        }
        return indexInfoList;
    }

    /**
     * Write table to disk in parquet format with {@link TableDefinition#getPartitioningColumns() partitioning columns}
     * written as "key=value" format in a nested directory structure. To generate these individual partitions, this
     * method will call {@link Table#partitionBy(String...) partitionBy} on all the partitioning columns of provided
     * table. The generated parquet files will have names of the format provided by
     * {@link ParquetInstructions#baseNameForPartitionedParquetData()}. By default, any indexing columns present on the
     * source table will be written as sidecar tables. To write only a subset of the indexes or add additional indexes
     * while writing, use {@link ParquetInstructions.Builder#addIndexColumns}.
     *
     * @param sourceTable The table to partition and write
     * @param destinationDir The path to destination root directory to store partitioned data in nested format.
     *        Non-existing directories are created.
     * @param writeInstructions Write instructions for customizations while writing
     */
    public static void writeKeyValuePartitionedTable(
            @NotNull final Table sourceTable,
            @NotNull final String destinationDir,
            @NotNull final ParquetInstructions writeInstructions) {
        final Collection<List<String>> indexColumns =
                writeInstructions.getIndexColumns().orElseGet(() -> indexedColumnNames(sourceTable));
        final TableDefinition definition = writeInstructions.getTableDefinition().orElse(sourceTable.getDefinition());
        final List<ColumnDefinition<?>> partitioningColumns = definition.getPartitioningColumns();
        if (partitioningColumns.isEmpty()) {
            throw new IllegalArgumentException("Table must have partitioning columns to write partitioned data");
        }
        final String[] partitioningColNames = partitioningColumns.stream()
                .map(ColumnDefinition::getName)
                .toArray(String[]::new);
        final PartitionedTable partitionedTable = sourceTable.partitionBy(partitioningColNames);
        final TableDefinition keyTableDefinition = TableDefinition.of(partitioningColumns);
        final TableDefinition leafDefinition =
                getNonKeyTableDefinition(new HashSet<>(Arrays.asList(partitioningColNames)), definition);
        writeKeyValuePartitionedTableImpl(partitionedTable, keyTableDefinition, leafDefinition, destinationDir,
                writeInstructions, indexColumns, Optional.of(sourceTable));
    }

    /**
     * Write a partitioned table to disk in parquet format with all the {@link PartitionedTable#keyColumnNames() key
     * columns} as "key=value" format in a nested directory structure. To generate the partitioned table, users can call
     * {@link Table#partitionBy(String...) partitionBy} on the required columns. The generated parquet files will have
     * names of the format provided by {@link ParquetInstructions#baseNameForPartitionedParquetData()}. By default, this
     * method does not write any indexes as sidecar tables to disk. To write such indexes, use
     * {@link ParquetInstructions.Builder#addIndexColumns}.
     *
     * @param partitionedTable The partitioned table to write
     * @param destinationDir The path to destination root directory to store partitioned data in nested format.
     *        Non-existing directories are created.
     * @param writeInstructions Write instructions for customizations while writing
     */
    public static void writeKeyValuePartitionedTable(
            @NotNull final PartitionedTable partitionedTable,
            @NotNull final String destinationDir,
            @NotNull final ParquetInstructions writeInstructions) {
        final Collection<List<String>> indexColumns = writeInstructions.getIndexColumns().orElse(EMPTY_INDEXES);
        final TableDefinition keyTableDefinition, leafDefinition;
        if (writeInstructions.getTableDefinition().isEmpty()) {
            keyTableDefinition = getKeyTableDefinition(partitionedTable.keyColumnNames(),
                    partitionedTable.table().getDefinition());
            leafDefinition = getNonKeyTableDefinition(partitionedTable.keyColumnNames(),
                    partitionedTable.constituentDefinition());
        } else {
            final TableDefinition definition = writeInstructions.getTableDefinition().get();
            keyTableDefinition = getKeyTableDefinition(partitionedTable.keyColumnNames(), definition);
            leafDefinition = getNonKeyTableDefinition(partitionedTable.keyColumnNames(), definition);
        }
        writeKeyValuePartitionedTableImpl(partitionedTable, keyTableDefinition, leafDefinition, destinationDir,
                writeInstructions, indexColumns, Optional.empty());
    }

    /**
     * Write a partitioned table to disk in a key=value partitioning format with the already computed definition for the
     * key table and leaf table.
     *
     * @param partitionedTable The partitioned table to write
     * @param keyTableDefinition The definition for key columns
     * @param leafDefinition The definition for leaf parquet files to be written
     * @param destinationRoot The path to destination root directory to store partitioned data in nested format
     * @param writeInstructions Write instructions for customizations while writing
     * @param indexColumns Collection containing the column names for indexes to persist. The write operation will store
     *        the index info as sidecar tables. This argument is used to narrow the set of indexes to write, or to be
     *        explicit about the expected set of indexes present on all sources. Indexes that are specified but missing
     *        will be computed on demand.
     * @param sourceTable The optional source table, provided when user provides a merged source table to write, like in
     *        {@link #writeKeyValuePartitionedTable(Table, String, ParquetInstructions)}
     */
    private static void writeKeyValuePartitionedTableImpl(
            @NotNull final PartitionedTable partitionedTable,
            @NotNull final TableDefinition keyTableDefinition,
            @NotNull final TableDefinition leafDefinition,
            @NotNull final String destinationRoot,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final Collection<List<String>> indexColumns,
            @NotNull final Optional<Table> sourceTable) {
        if (leafDefinition.numColumns() == 0) {
            throw new IllegalArgumentException("Cannot write a partitioned parquet table without any non-partitioning "
                    + "columns");
        }
        final String baseName = writeInstructions.baseNameForPartitionedParquetData();
        final boolean hasPartitionInName = baseName.contains(PARTITIONS_TOKEN);
        final boolean hasIndexInName = baseName.contains(FILE_INDEX_TOKEN);
        final boolean hasUUIDInName = baseName.contains(UUID_TOKEN);
        if (!partitionedTable.uniqueKeys() && !hasIndexInName && !hasUUIDInName) {
            throw new IllegalArgumentException(
                    "Cannot write a partitioned parquet table with non-unique keys without {i} or {uuid} in the base " +
                            "name because there can be multiple partitions with the same key values");
        }
        // Note that there can be multiple constituents with the same key values, so cannot directly use the
        // partitionedTable.constituentFor(keyValues) method, and we need to group them together
        final String[] partitioningColumnNames = partitionedTable.keyColumnNames().toArray(String[]::new);
        final Table withGroupConstituents = partitionedTable.table().groupBy(partitioningColumnNames);
        // For each row, accumulate the partition values in a key=value format
        final List<List<String>> partitionStringsList = new ArrayList<>();
        final long numRows = withGroupConstituents.size();
        for (long i = 0; i < numRows; i++) {
            partitionStringsList.add(new ArrayList<>(partitioningColumnNames.length));
        }
        Arrays.stream(partitioningColumnNames).forEach(columnName -> {
            final PartitionFormatter partitionFormatter = PartitionFormatter.getFormatterForType(
                    withGroupConstituents.getColumnSource(columnName).getType());
            try (final CloseableIterator<?> valueIterator = withGroupConstituents.columnIterator(columnName)) {
                int row = 0;
                while (valueIterator.hasNext()) {
                    final String partitioningValue = partitionFormatter.format(valueIterator.next());
                    partitionStringsList.get(row).add(columnName + "=" + partitioningValue);
                    row++;
                }
            }
        });
        // For the constituent column for each row, accumulate the constituent tables and build the final file paths
        final Collection<Table> partitionedData = new ArrayList<>();
        final Collection<File> destinations = new ArrayList<>();
        try (final CloseableIterator<ObjectVector<? extends Table>> constituentIterator =
                withGroupConstituents.objectColumnIterator(partitionedTable.constituentColumnName())) {
            int row = 0;
            while (constituentIterator.hasNext()) {
                final ObjectVector<? extends Table> constituentVector = constituentIterator.next();
                final List<String> partitionStrings = partitionStringsList.get(row);
                final File relativePath = new File(destinationRoot, String.join(File.separator, partitionStrings));
                int count = 0;
                for (final Table constituent : constituentVector) {
                    String filename = baseName;
                    if (hasPartitionInName) {
                        filename = baseName.replace(PARTITIONS_TOKEN, String.join("_", partitionStrings));
                    }
                    if (hasIndexInName) {
                        filename = filename.replace(FILE_INDEX_TOKEN, Integer.toString(count));
                    }
                    if (hasUUIDInName) {
                        filename = filename.replace(UUID_TOKEN, UUID.randomUUID().toString());
                    }
                    filename += PARQUET_FILE_EXTENSION;
                    destinations.add(new File(relativePath, filename));
                    partitionedData.add(constituent);
                    count++;
                }
                row++;
            }
        }
        final MessageType partitioningColumnsSchema;
        if (writeInstructions.generateMetadataFiles()) {
            // Generate schema for partitioning columns for _common_metadata file. The schema for remaining columns will
            // be inferred at the time of writing the parquet files and merged with the common schema.
            partitioningColumnsSchema =
                    getSchemaForTable(partitionedTable.table(), keyTableDefinition, writeInstructions);
        } else {
            partitioningColumnsSchema = null;
        }
        final Table[] partitionedDataArray = partitionedData.toArray(Table[]::new);
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final Map<String, Map<ParquetCacheTags, Object>> computedCache =
                    buildComputedCache(() -> sourceTable.orElseGet(partitionedTable::merge), leafDefinition);
            // TODO(deephaven-core#5292): Optimize creating index on constituent tables
            // Store hard reference to prevent indexes from being garbage collected
            final List<DataIndex> dataIndexes = addIndexesToTables(partitionedDataArray, indexColumns);
            writeTablesImpl(partitionedDataArray, leafDefinition, writeInstructions,
                    destinations.toArray(File[]::new), indexColumns, partitioningColumnsSchema,
                    new File(destinationRoot), computedCache);
            if (dataIndexes != null) {
                dataIndexes.clear();
            }
        }
    }

    /**
     * Add data indexes to provided tables, if not present, and return a list of hard references to the indexes.
     */
    @Nullable
    private static List<DataIndex> addIndexesToTables(
            @NotNull final Table[] tables,
            @NotNull final Collection<List<String>> indexColumns) {
        if (indexColumns.isEmpty()) {
            return null;
        }
        final List<DataIndex> dataIndexes = new ArrayList<>(indexColumns.size() * tables.length);
        for (final Table table : tables) {
            for (final List<String> indexCols : indexColumns) {
                dataIndexes.add(DataIndexer.getOrCreateDataIndex(table, indexCols));
            }
        }
        return dataIndexes;
    }

    /**
     * Using the provided definition and key column names, create a sub table definition for the key columns that are
     * present in the definition.
     */
    private static TableDefinition getKeyTableDefinition(
            @NotNull final Collection<String> keyColumnNames,
            @NotNull final TableDefinition definition) {
        final Collection<ColumnDefinition<?>> keyColumnDefinitions = new ArrayList<>(keyColumnNames.size());
        for (final String keyColumnName : keyColumnNames) {
            final ColumnDefinition<?> keyColumnDef = definition.getColumn(keyColumnName);
            if (keyColumnDef != null) {
                keyColumnDefinitions.add(keyColumnDef);
            }
        }
        return TableDefinition.of(keyColumnDefinitions);
    }

    /**
     * Using the provided definition and key column names, create a sub table definition for the non-key columns.
     */
    private static TableDefinition getNonKeyTableDefinition(
            @NotNull final Collection<String> keyColumnNames,
            @NotNull final TableDefinition definition) {
        final Collection<ColumnDefinition<?>> nonKeyColumnDefinition = definition.getColumns().stream()
                .filter(columnDefinition -> !keyColumnNames.contains(columnDefinition.getName()))
                .collect(Collectors.toList());
        return TableDefinition.of(nonKeyColumnDefinition);
    }

    /**
     * If the definition has any big decimal columns, precompute the precision and scale values for big decimal columns
     * for the merged table so that all the constituent parquet files are written with the same schema, precision and
     * scale values. We only need to perform the merge operation if there is a big decimal column in the definition.
     * That is why this method accepts a supplier instead of the table itself.
     */
    private static Map<String, Map<ParquetCacheTags, Object>> buildComputedCache(
            @NotNull final Supplier<Table> mergedTableSupplier,
            @NotNull final TableDefinition definition) {
        final Map<String, Map<ParquetCacheTags, Object>> computedCache = new HashMap<>();
        Table mergedTable = null;
        final List<ColumnDefinition<?>> leafColumnDefinitions = definition.getColumns();
        for (final ColumnDefinition<?> columnDefinition : leafColumnDefinitions) {
            if (columnDefinition.getDataType() == BigDecimal.class) {
                if (mergedTable == null) {
                    mergedTable = mergedTableSupplier.get();
                }
                final String columnName = columnDefinition.getName();
                final ColumnSource<BigDecimal> bigDecimalColumnSource = mergedTable.getColumnSource(columnName);
                TypeInfos.getPrecisionAndScale(computedCache, columnName, mergedTable.getRowSet(),
                        () -> bigDecimalColumnSource);
            }
        }
        return computedCache;
    }

    /**
     * Refer to {@link #writeTables(Table[], String[], ParquetInstructions)} for more details.
     */
    private static void writeTablesImpl(
            @NotNull final Table[] sources,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final File[] destinations,
            @NotNull final Collection<List<String>> indexColumns,
            @Nullable final MessageType partitioningColumnsSchema,
            @Nullable final File metadataRootDir,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache) {
        Require.eq(sources.length, "sources.length", destinations.length, "destinations.length");
        if (writeInstructions.getFileLayout().isPresent()) {
            throw new UnsupportedOperationException("File layout is not supported for writing parquet files, use the " +
                    "appropriate API");
        }
        if (definition.numColumns() == 0) {
            throw new TableDataException("Cannot write a parquet table with zero columns");
        }
        Arrays.stream(destinations).forEach(ParquetTools::deleteBackupFile);

        // Write all files at temporary shadow file paths in the same directory to prevent overwriting any existing
        // data in case of failure
        final File[] shadowDestFiles =
                Arrays.stream(destinations).map(ParquetTools::getShadowFile).toArray(File[]::new);
        final File[] firstCreatedDirs =
                Arrays.stream(shadowDestFiles).map(ParquetTools::prepareDestinationFileLocation).toArray(File[]::new);

        final ParquetMetadataFileWriter metadataFileWriter;
        if (writeInstructions.generateMetadataFiles()) {
            if (metadataRootDir == null) {
                throw new IllegalArgumentException("Metadata root directory must be set when writing metadata files");
            }
            metadataFileWriter =
                    new ParquetMetadataFileWriterImpl(metadataRootDir, destinations, partitioningColumnsSchema);
        } else {
            metadataFileWriter = NullParquetMetadataFileWriter.INSTANCE;
        }

        // List of shadow files, to clean up in case of exceptions
        final List<File> shadowFiles = new ArrayList<>();
        // List of all destination files (including index files), to roll back in case of exceptions
        final List<File> destFiles = new ArrayList<>();
        try {
            final List<List<ParquetTableWriter.IndexWritingInfo>> indexInfoLists;
            if (indexColumns.isEmpty()) {
                // Write the tables without any index info
                indexInfoLists = null;
                for (int tableIdx = 0; tableIdx < sources.length; tableIdx++) {
                    shadowFiles.add(shadowDestFiles[tableIdx]);
                    final Table source = sources[tableIdx];
                    ParquetTableWriter.write(source, definition, writeInstructions, shadowDestFiles[tableIdx].getPath(),
                            destinations[tableIdx].getPath(), Collections.emptyMap(),
                            (List<ParquetTableWriter.IndexWritingInfo>) null, metadataFileWriter,
                            computedCache);
                }
            } else {
                // Create index info for each table and write the table and index files to shadow path
                indexInfoLists = new ArrayList<>(sources.length);

                // Shared parquet column names across all tables
                final String[][] parquetColumnNameArr = indexColumns.stream()
                        .map((Collection<String> columns) -> columns.stream()
                                .map(writeInstructions::getParquetColumnNameFromColumnNameOrDefault)
                                .toArray(String[]::new))
                        .toArray(String[][]::new);

                for (int tableIdx = 0; tableIdx < sources.length; tableIdx++) {
                    final File tableDestination = destinations[tableIdx];
                    final List<ParquetTableWriter.IndexWritingInfo> indexInfoList =
                            indexInfoBuilderHelper(indexColumns, parquetColumnNameArr, tableDestination);
                    indexInfoLists.add(indexInfoList);

                    shadowFiles.add(shadowDestFiles[tableIdx]);
                    indexInfoList.forEach(item -> shadowFiles.add(item.destFile));

                    final Table sourceTable = sources[tableIdx];
                    ParquetTableWriter.write(sourceTable, definition, writeInstructions,
                            shadowDestFiles[tableIdx].getPath(), tableDestination.getPath(), Collections.emptyMap(),
                            indexInfoList, metadataFileWriter, computedCache);
                }
            }

            // Write the combined metadata files to shadow destinations
            final File metadataDestFile, shadowMetadataFile, commonMetadataDestFile, shadowCommonMetadataFile;
            if (writeInstructions.generateMetadataFiles()) {
                metadataDestFile = new File(metadataRootDir, METADATA_FILE_NAME);
                shadowMetadataFile = ParquetTools.getShadowFile(metadataDestFile);
                shadowFiles.add(shadowMetadataFile);
                commonMetadataDestFile = new File(metadataRootDir, COMMON_METADATA_FILE_NAME);
                shadowCommonMetadataFile = ParquetTools.getShadowFile(commonMetadataDestFile);
                shadowFiles.add(shadowCommonMetadataFile);
                metadataFileWriter.writeMetadataFiles(shadowMetadataFile.getAbsolutePath(),
                        shadowCommonMetadataFile.getAbsolutePath());
            } else {
                metadataDestFile = shadowMetadataFile = commonMetadataDestFile = shadowCommonMetadataFile = null;
            }

            // Write to shadow files was successful, now replace the original files with the shadow files
            for (int tableIdx = 0; tableIdx < sources.length; tableIdx++) {
                destFiles.add(destinations[tableIdx]);
                installShadowFile(destinations[tableIdx], shadowDestFiles[tableIdx]);
                if (indexInfoLists != null) {
                    final List<ParquetTableWriter.IndexWritingInfo> indexInfoList = indexInfoLists.get(tableIdx);
                    for (final ParquetTableWriter.IndexWritingInfo info : indexInfoList) {
                        final File indexDestFile = info.destFileForMetadata;
                        final File shadowIndexFile = info.destFile;
                        destFiles.add(indexDestFile);
                        installShadowFile(indexDestFile, shadowIndexFile);
                    }
                }
            }
            if (writeInstructions.generateMetadataFiles()) {
                destFiles.add(metadataDestFile);
                installShadowFile(metadataDestFile, shadowMetadataFile);
                destFiles.add(commonMetadataDestFile);
                installShadowFile(commonMetadataDestFile, shadowCommonMetadataFile);
            }
        } catch (Exception e) {
            for (final File file : destFiles) {
                rollbackFile(file);
            }
            for (final File file : shadowFiles) {
                file.delete();
            }
            for (final File firstCreatedDir : firstCreatedDirs) {
                if (firstCreatedDir == null) {
                    continue;
                }
                log.error().append(
                        "Error in table writing, cleaning up potentially incomplete table destination path starting from ")
                        .append(firstCreatedDir.getAbsolutePath()).append(e).endl();
                FileUtils.deleteRecursivelyOnNFS(firstCreatedDir);
            }
            throw new UncheckedDeephavenException("Error writing parquet tables", e);
        }
        destFiles.forEach(ParquetTools::deleteBackupFileNoExcept);
    }

    /**
     * Examine the source tables to retrieve the list of indexes as String lists.
     *
     * @param sources The tables from which to retrieve the indexes
     * @return A {@link Collection} containing the indexes as String lists
     * @implNote This only examines the first source table. The writing code will compute missing indexes for the other
     *           source tables.
     */
    @NotNull
    private static Collection<List<String>> indexedColumnNames(@NotNull final Table @NotNull [] sources) {
        if (sources.length == 0) {
            return EMPTY_INDEXES;
        }
        // Use the first table as the source of indexed columns
        return indexedColumnNames(sources[0]);
    }

    /**
     * Examine the source table to retrieve the list of indexes as String lists.
     *
     * @param source The table from which to retrieve the indexes
     * @return A {@link Collection} containing the indexes as String lists.
     */
    @NotNull
    private static Collection<List<String>> indexedColumnNames(@NotNull final Table source) {
        final DataIndexer dataIndexer = DataIndexer.existingOf(source.getRowSet());
        if (dataIndexer == null) {
            return EMPTY_INDEXES;
        }
        final List<DataIndex> dataIndexes = dataIndexer.dataIndexes(true);
        if (dataIndexes.isEmpty()) {
            return EMPTY_INDEXES;
        }
        final Map<String, ? extends ColumnSource<?>> nameToColumn = source.getColumnSourceMap();
        // We disregard collisions, here; any mapped name is an adequate choice.
        final Map<ColumnSource<?>, String> columnToName = nameToColumn.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        final Collection<List<String>> indexesToWrite = new ArrayList<>();

        // Build the list of indexes to write
        dataIndexes.forEach(di -> {
            final Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn = di.keyColumnNamesByIndexedColumn();

            // Re-map the index columns to their names in this table
            final List<String> keyColumnNames = keyColumnNamesByIndexedColumn.keySet().stream()
                    .map(columnToName::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toUnmodifiableList());

            // Make sure all the columns actually exist in the table
            if (keyColumnNames.size() == keyColumnNamesByIndexedColumn.size()) {
                indexesToWrite.add(keyColumnNames);
            }
        });
        return Collections.unmodifiableCollection(indexesToWrite);
    }

    /**
     * Write out tables to disk. Data indexes to write are determined by those already present on the first source or
     * those provided through {@link ParquetInstructions.Builder#addIndexColumns}. If all source tables have the same
     * definition, this method will use the common definition for writing. Else, a definition must be provided through
     * the {@code writeInstructions}.
     *
     * @param sources The tables to write
     * @param destinations The destination paths or URIs. Any non-existing directories in the paths provided are
     *        created. If there is an error, any intermediate directories previously created are removed; note this
     *        makes this method unsafe for concurrent use.
     * @param writeInstructions Write instructions for customizations while writing
     */
    public static void writeTables(
            @NotNull final Table[] sources,
            @NotNull final String[] destinations,
            @NotNull final ParquetInstructions writeInstructions) {
        if (sources.length == 0) {
            throw new IllegalArgumentException("No source tables provided for writing");
        }
        if (sources.length != destinations.length) {
            throw new IllegalArgumentException("Number of sources and destinations must match");
        }
        final TableDefinition definition;
        if (writeInstructions.getTableDefinition().isPresent()) {
            definition = writeInstructions.getTableDefinition().get();
        } else {
            final TableDefinition firstDefinition = sources[0].getDefinition();
            for (int idx = 1; idx < sources.length; idx++) {
                if (!firstDefinition.equals(sources[idx].getDefinition())) {
                    throw new IllegalArgumentException(
                            "Table definition must be provided when writing multiple tables " +
                                    "with different definitions");
                }
            }
            definition = firstDefinition;
        }
        final File[] destinationFiles = new File[destinations.length];
        for (int idx = 0; idx < destinations.length; idx++) {
            final URI destinationURI = convertToURI(destinations[idx], false);
            if (!FILE_URI_SCHEME.equals(destinationURI.getScheme())) {
                throw new IllegalArgumentException(
                        "Only file URI scheme is supported for writing parquet files, found" +
                                "non-file URI: " + destinations[idx]);
            }
            destinationFiles[idx] = new File(destinationURI);
        }
        final File metadataRootDir;
        if (writeInstructions.generateMetadataFiles()) {
            // We insist on writing the metadata file in the same directory as the destination files, thus all
            // destination files should be in the same directory.
            final String firstDestinationDir = destinationFiles[0].getAbsoluteFile().getParentFile().getAbsolutePath();
            for (int i = 1; i < destinations.length; i++) {
                if (!firstDestinationDir.equals(destinationFiles[i].getParentFile().getAbsolutePath())) {
                    throw new IllegalArgumentException("All destination files must be in the same directory for " +
                            " generating metadata files");
                }
            }
            metadataRootDir = new File(firstDestinationDir);
        } else {
            metadataRootDir = null;
        }
        final Collection<List<String>> indexColumns =
                writeInstructions.getIndexColumns().orElseGet(() -> indexedColumnNames(sources));
        final Map<String, Map<ParquetCacheTags, Object>> computedCache =
                buildComputedCache(() -> PartitionedTableFactory.ofTables(definition, sources).merge(), definition);
        // We do not have any additional schema for partitioning columns in this case. Schema for all columns will be
        // generated at the time of writing the parquet files and merged to generate the metadata files.
        writeTablesImpl(sources, definition, writeInstructions, destinationFiles, indexColumns, null, metadataRootDir,
                computedCache);
    }

    /**
     * Deletes a table on disk.
     *
     * @param path path to delete
     */
    @VisibleForTesting
    public static void deleteTable(final String path) {
        FileUtils.deleteRecursivelyOnNFS(new File(path));
    }

    /**
     * This method attempts to "do the right thing." It examines the source file URI to determine if it's a single
     * parquet file, a metadata file, or a directory. If it's a directory, it additionally tries to guess the layout to
     * use. Unless a metadata file is supplied or discovered in the directory, the highest (by
     * {@link ParquetTableLocationKey location key} order) location found will be used to infer schema.
     *
     * @param source The source URI with {@value ParquetFileReader#FILE_URI_SCHEME} scheme
     * @param instructions Instructions for reading
     * @return A {@link Table}
     */
    private static Table readTableFromFileUri(
            @NotNull final URI source,
            @NotNull final ParquetInstructions instructions) {
        final Path sourcePath = Path.of(source);
        if (!Files.exists(sourcePath)) {
            throw new TableDataException("Source file " + source + " does not exist");
        }
        final String sourceFileName = sourcePath.getFileName().toString();
        final BasicFileAttributes sourceAttr = readAttributes(sourcePath);
        if (sourceAttr.isRegularFile()) {
            if (sourceFileName.endsWith(PARQUET_FILE_EXTENSION)) {
                return readSingleFileTable(source, instructions);
            }
            final URI parentDirURI = convertToURI(sourcePath.getParent(), true);
            if (sourceFileName.equals(METADATA_FILE_NAME)) {
                return readPartitionedTableWithMetadata(parentDirURI, instructions);
            }
            if (sourceFileName.equals(COMMON_METADATA_FILE_NAME)) {
                return readPartitionedTableWithMetadata(parentDirURI, instructions);
            }
            throw new TableDataException(
                    "Source file " + source + " does not appear to be a parquet file or metadata file");
        }
        if (sourceAttr.isDirectory()) {
            final Path metadataPath = sourcePath.resolve(METADATA_FILE_NAME);
            if (Files.exists(metadataPath)) {
                return readPartitionedTableWithMetadata(source, instructions);
            }
            final Path firstEntryPath;
            // Ignore dot files while looking for the first entry
            try (final DirectoryStream<Path> sourceStream =
                    Files.newDirectoryStream(sourcePath, ParquetTools::ignoreDotFiles)) {
                final Iterator<Path> entryIterator = sourceStream.iterator();
                if (!entryIterator.hasNext()) {
                    throw new TableDataException("Source directory " + source + " is empty");
                }
                firstEntryPath = entryIterator.next();
            } catch (IOException e) {
                throw new TableDataException("Error reading source directory " + source, e);
            }
            final String firstEntryFileName = firstEntryPath.getFileName().toString();
            final BasicFileAttributes firstEntryAttr = readAttributes(firstEntryPath);
            if (firstEntryAttr.isDirectory() && firstEntryFileName.contains("=")) {
                return readKeyValuePartitionedTable(source, instructions);
            }
            if (firstEntryAttr.isRegularFile() && firstEntryFileName.endsWith(PARQUET_FILE_EXTENSION)) {
                return readFlatPartitionedTable(source, instructions);
            }
            throw new TableDataException("No recognized Parquet table layout found in " + source);
        }
        throw new TableDataException("Source " + source + " is neither a directory nor a regular file");
    }

    private static boolean ignoreDotFiles(Path path) {
        final String filename = path.getFileName().toString();
        return !filename.isEmpty() && filename.charAt(0) != '.';
    }

    private static BasicFileAttributes readAttributes(@NotNull final Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            throw new TableDataException("Failed to read " + path + " file attributes", e);
        }
    }

    /**
     * Reads in a table from a single parquet file using the table definition provided through the
     * {@link ParquetInstructions}.
     *
     * <p>
     * Callers may prefer the simpler methods {@link #readTable(String, ParquetInstructions)} with layout provided as
     * {@link ParquetFileLayout#SINGLE_FILE} using {@link ParquetInstructions.Builder#setFileLayout} and
     * {@link TableDefinition} provided through {@link ParquetInstructions.Builder#setTableDefinition}.
     *
     * @param tableLocationKey The {@link ParquetTableLocationKey location keys} to include
     * @param readInstructions Instructions for customizations while reading
     * @return The table
     */
    private static Table readTable(
            @NotNull final ParquetTableLocationKey tableLocationKey,
            @NotNull final ParquetInstructions readInstructions) {
        if (readInstructions.isRefreshing()) {
            throw new IllegalArgumentException("Unable to have a refreshing single parquet file");
        }
        final TableDefinition tableDefinition = readInstructions.getTableDefinition().orElseThrow(
                () -> new IllegalArgumentException("Table definition must be provided"));
        verifyFileLayout(readInstructions, ParquetFileLayout.SINGLE_FILE);
        final TableLocationProvider locationProvider = new PollingTableLocationProvider<>(
                StandaloneTableKey.getInstance(),
                new KnownLocationKeyFinder<>(tableLocationKey),
                new ParquetTableLocationFactory(readInstructions),
                null);
        return new SimpleSourceTable(tableDefinition.getWritable(),
                "Read single parquet file from " + tableLocationKey.getURI(),
                RegionedTableComponentFactoryImpl.INSTANCE, locationProvider, null);
    }

    /**
     * Reads in a table from files discovered with {@code locationKeyFinder} using a definition either provided using
     * {@link ParquetInstructions} or built from the highest (by {@link ParquetTableLocationKey location key} order)
     * location found, which must have non-null partition values for all partition keys.
     *
     * <p>
     * Callers may prefer the simpler methods {@link #readTable(String, ParquetInstructions)} with layout provided using
     * {@link ParquetInstructions.Builder#setFileLayout}.
     *
     * @param locationKeyFinder The source of {@link ParquetTableLocationKey location keys} to include
     * @param readInstructions Instructions for customizations while reading
     * @return The table
     */
    public static Table readTable(
            @NotNull final TableLocationKeyFinder<ParquetTableLocationKey> locationKeyFinder,
            @NotNull final ParquetInstructions readInstructions) {
        final TableDefinition definition;
        final ParquetInstructions useInstructions;
        final TableLocationKeyFinder<ParquetTableLocationKey> useLocationKeyFinder;
        if (readInstructions.getTableDefinition().isEmpty()) {
            // Infer the definition
            final KnownLocationKeyFinder<ParquetTableLocationKey> inferenceKeys = toKnownKeys(locationKeyFinder);
            final Pair<TableDefinition, ParquetInstructions> inference = infer(inferenceKeys, readInstructions);
            // In the case of a static output table, we can re-use the already fetched inference keys
            useLocationKeyFinder = readInstructions.isRefreshing() ? locationKeyFinder : inferenceKeys;
            definition = inference.getFirst();
            useInstructions = inference.getSecond();
        } else {
            definition = readInstructions.getTableDefinition().get();
            useInstructions = readInstructions;
            useLocationKeyFinder = locationKeyFinder;
        }
        final String description;
        final TableLocationKeyFinder<ParquetTableLocationKey> keyFinder;
        final TableDataRefreshService refreshService;
        final UpdateSourceRegistrar updateSourceRegistrar;
        if (useInstructions.isRefreshing()) {
            keyFinder = useLocationKeyFinder;
            description = "Read refreshing parquet files with " + keyFinder;
            refreshService = TableDataRefreshService.getSharedRefreshService();
            updateSourceRegistrar = ExecutionContext.getContext().getUpdateGraph();
        } else {
            keyFinder = toKnownKeys(useLocationKeyFinder);
            description = "Read multiple parquet files with " + keyFinder;
            refreshService = null;
            updateSourceRegistrar = null;
        }
        return new PartitionAwareSourceTable(
                definition,
                description,
                RegionedTableComponentFactoryImpl.INSTANCE,
                new PollingTableLocationProvider<>(
                        StandaloneTableKey.getInstance(),
                        keyFinder,
                        new ParquetTableLocationFactory(useInstructions),
                        refreshService),
                updateSourceRegistrar);
    }

    private static Pair<TableDefinition, ParquetInstructions> infer(
            final KnownLocationKeyFinder<ParquetTableLocationKey> inferenceKeys,
            final ParquetInstructions readInstructions) {
        // TODO(deephaven-core#877): Support schema merge when discovering multiple parquet files
        final ParquetTableLocationKey lastKey = inferenceKeys.getLastKey().orElse(null);
        if (lastKey == null) {
            throw new IllegalArgumentException(
                    "Unable to infer schema for a partitioned parquet table when there are no initial parquet files");
        }
        final Pair<List<ColumnDefinition<?>>, ParquetInstructions> schemaInfo = ParquetSchemaReader.convertSchema(
                lastKey.getFileReader().getSchema(),
                lastKey.getMetadata().getFileMetaData().getKeyValueMetaData(),
                readInstructions);
        final Set<String> partitionKeys = lastKey.getPartitionKeys();
        final List<ColumnDefinition<?>> allColumns =
                new ArrayList<>(partitionKeys.size() + schemaInfo.getFirst().size());
        for (final String partitionKey : partitionKeys) {
            final Comparable<?> partitionValue = lastKey.getPartitionValue(partitionKey);
            if (partitionValue == null) {
                throw new IllegalArgumentException(String.format(
                        "Last location key %s has null partition value at partition key %s", lastKey, partitionKey));
            }
            // Primitives should be unboxed, except booleans
            Class<?> dataType = partitionValue.getClass();
            if (dataType != Boolean.class) {
                dataType = getUnboxedTypeIfBoxed(partitionValue.getClass());
            }
            allColumns.add(ColumnDefinition.fromGenericType(partitionKey, dataType, null,
                    ColumnDefinition.ColumnType.Partitioning));
        }
        // Only read non-partitioning columns from the parquet files
        final List<ColumnDefinition<?>> columnDefinitionsFromParquetFile = schemaInfo.getFirst();
        columnDefinitionsFromParquetFile.stream()
                .filter(columnDefinition -> !partitionKeys.contains(columnDefinition.getName()))
                .forEach(allColumns::add);
        return new Pair<>(TableDefinition.of(allColumns), schemaInfo.getSecond());
    }

    private static KnownLocationKeyFinder<ParquetTableLocationKey> toKnownKeys(
            TableLocationKeyFinder<ParquetTableLocationKey> keyFinder) {
        return keyFinder instanceof KnownLocationKeyFinder
                ? (KnownLocationKeyFinder<ParquetTableLocationKey>) keyFinder
                : KnownLocationKeyFinder.copyFrom(keyFinder, Comparator.naturalOrder());
    }

    private static Table readPartitionedTableWithMetadata(
            @NotNull final URI sourceURI,
            @NotNull final ParquetInstructions readInstructions) {
        if (!FILE_URI_SCHEME.equals(sourceURI.getScheme())) {
            throw new UnsupportedOperationException("Reading metadata files from non local storage is not supported");
        }
        verifyFileLayout(readInstructions, ParquetFileLayout.METADATA_PARTITIONED);
        if (readInstructions.getTableDefinition().isPresent()) {
            throw new UnsupportedOperationException("Detected table definition inside read instructions, reading " +
                    "metadata files with custom table definition is currently not supported");
        }
        final File sourceFile = new File(sourceURI);
        final String fileName = sourceFile.getName();
        final File directory;
        if (fileName.equals(METADATA_FILE_NAME) || fileName.equals(COMMON_METADATA_FILE_NAME)) {
            directory = sourceFile.getParentFile();
        } else {
            directory = sourceFile;
        }
        final ParquetMetadataFileLayout layout = new ParquetMetadataFileLayout(directory, readInstructions);
        return readTable(layout,
                ensureTableDefinition(layout.getInstructions(), layout.getTableDefinition(), true));
    }

    private static void verifyFileLayout(
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final ParquetFileLayout expectedLayout) {
        if (readInstructions.getFileLayout().isPresent() && readInstructions.getFileLayout().get() != expectedLayout) {
            throw new IllegalArgumentException("File layout provided in read instructions (=" +
                    readInstructions.getFileLayout() + ") does not match with " + expectedLayout);
        }
    }

    /**
     * Creates a partitioned table via the key-value partitioned parquet files from the root {@code directory},
     * inferring the table definition from those files.
     * <p>
     * Callers wishing to be more explicit and skip the inference step should provide a {@link TableDefinition} as part
     * of read instructions using {@link ParquetInstructions.Builder#setTableDefinition}.
     *
     * @param directoryUri the URI for the root directory to search for .parquet files
     * @param readInstructions the instructions for customizations while reading
     * @return the table
     */
    private static Table readKeyValuePartitionedTable(
            @NotNull final URI directoryUri,
            @NotNull final ParquetInstructions readInstructions) {
        verifyFileLayout(readInstructions, ParquetFileLayout.KV_PARTITIONED);
        if (readInstructions.getTableDefinition().isEmpty()) {
            return readTable(new ParquetKeyValuePartitionedLayout(directoryUri,
                    MAX_PARTITIONING_LEVELS_INFERENCE, readInstructions), readInstructions);
        }
        final TableDefinition tableDefinition = readInstructions.getTableDefinition().get();
        if (tableDefinition.getColumnStream().noneMatch(ColumnDefinition::isPartitioning)) {
            throw new IllegalArgumentException("No partitioning columns");
        }
        return readTable(new ParquetKeyValuePartitionedLayout(directoryUri, tableDefinition,
                readInstructions), readInstructions);
    }

    /**
     * Creates a partitioned table via the flat parquet files from the root {@code directory}, inferring the table
     * definition from those files.
     *
     * @param sourceURI the path or URI for the directory to search for .parquet files
     * @param readInstructions the instructions for customizations while reading
     * @return the table
     * @see #readTable(TableLocationKeyFinder, ParquetInstructions)
     * @see ParquetFlatPartitionedLayout#ParquetFlatPartitionedLayout(URI, ParquetInstructions)
     */
    private static Table readFlatPartitionedTable(
            @NotNull final URI sourceURI,
            @NotNull final ParquetInstructions readInstructions) {
        verifyFileLayout(readInstructions, ParquetFileLayout.FLAT_PARTITIONED);
        return readTable(new ParquetFlatPartitionedLayout(sourceURI, readInstructions), readInstructions);
    }

    private static Table readSingleFileTable(
            @NotNull final URI parquetFileURI,
            @NotNull final ParquetInstructions readInstructions) {
        verifyFileLayout(readInstructions, ParquetFileLayout.SINGLE_FILE);
        final ParquetTableLocationKey locationKey =
                new ParquetTableLocationKey(parquetFileURI, 0, null, readInstructions);
        if (readInstructions.getTableDefinition().isPresent()) {
            return readTable(locationKey, readInstructions);
        }
        // Infer the table definition
        final KnownLocationKeyFinder<ParquetTableLocationKey> inferenceKeys = new KnownLocationKeyFinder<>(locationKey);
        final Pair<TableDefinition, ParquetInstructions> inference = infer(inferenceKeys, readInstructions);
        final TableDefinition inferredTableDefinition = inference.getFirst();
        final ParquetInstructions inferredInstructions = inference.getSecond();
        return readTable(inferenceKeys.getFirstKey().orElseThrow(),
                ensureTableDefinition(inferredInstructions, inferredTableDefinition, true));
    }

    @VisibleForTesting
    public static Table readParquetSchemaAndTable(
            @NotNull final File source,
            @NotNull final ParquetInstructions readInstructionsIn,
            @Nullable final MutableObject<ParquetInstructions> mutableInstructionsOut) {
        final URI sourceURI = convertToURI(source, false);
        final ParquetTableLocationKey tableLocationKey =
                new ParquetTableLocationKey(sourceURI, 0, null, readInstructionsIn);
        final Pair<List<ColumnDefinition<?>>, ParquetInstructions> schemaInfo = ParquetSchemaReader.convertSchema(
                tableLocationKey.getFileReader().getSchema(),
                tableLocationKey.getMetadata().getFileMetaData().getKeyValueMetaData(),
                readInstructionsIn);
        final TableDefinition def = TableDefinition.of(schemaInfo.getFirst());
        final ParquetInstructions instructionsOut = ensureTableDefinition(schemaInfo.getSecond(), def, true);
        if (mutableInstructionsOut != null) {
            mutableInstructionsOut.setValue(instructionsOut);
        }
        return readTable(tableLocationKey, instructionsOut);
    }

    public static final ParquetInstructions UNCOMPRESSED =
            ParquetInstructions.builder().setCompressionCodecName("UNCOMPRESSED").build();

    // endregion

    /**
     * @deprecated Use LZ4_RAW instead, as explained
     *             <a href="https://github.com/apache/parquet-format/blob/master/Compression.md">here</a>
     */
    @Deprecated
    public static final ParquetInstructions LZ4 = ParquetInstructions.builder().setCompressionCodecName("LZ4").build();
    public static final ParquetInstructions LZ4_RAW =
            ParquetInstructions.builder().setCompressionCodecName("LZ4_RAW").build();
    public static final ParquetInstructions LZO = ParquetInstructions.builder().setCompressionCodecName("LZO").build();
    public static final ParquetInstructions GZIP =
            ParquetInstructions.builder().setCompressionCodecName("GZIP").build();
    public static final ParquetInstructions ZSTD =
            ParquetInstructions.builder().setCompressionCodecName("ZSTD").build();
    public static final ParquetInstructions SNAPPY =
            ParquetInstructions.builder().setCompressionCodecName("SNAPPY").build();
    public static final ParquetInstructions BROTLI =
            ParquetInstructions.builder().setCompressionCodecName("BROTLI").build();
    public static final ParquetInstructions LEGACY = ParquetInstructions.builder().setIsLegacyParquet(true).build();
}
