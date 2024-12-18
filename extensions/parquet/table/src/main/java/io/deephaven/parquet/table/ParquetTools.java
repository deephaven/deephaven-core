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
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.util.PartitionFormatter;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.parquet.base.ParquetMetadataFileWriter;
import io.deephaven.parquet.base.NullParquetMetadataFileWriter;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.channel.CompletableOutputStream;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
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
import io.deephaven.parquet.base.ParquetUtils;
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
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.deephaven.base.FileUtils.URI_SEPARATOR_CHAR;
import static io.deephaven.base.FileUtils.convertToURI;
import static io.deephaven.parquet.base.ParquetUtils.PARQUET_OUTPUT_BUFFER_SIZE;
import static io.deephaven.parquet.base.ParquetUtils.resolve;
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
     * provided can be a local file path or a URI to be resolved.
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
     * provided can be a local file path or a URI to be resolved.
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
        final boolean isParquetFile = ParquetUtils.isParquetFile(source);
        final boolean isMetadataFile = !isParquetFile && ParquetUtils.isMetadataFile(source);
        final boolean isDirectory = !isParquetFile && !isMetadataFile;
        final URI sourceURI = convertToURI(source, isDirectory);
        if (readInstructions.getFileLayout().isPresent()) {
            switch (readInstructions.getFileLayout().get()) {
                case SINGLE_FILE:
                    return readSingleFileTable(sourceURI, readInstructions);
                case FLAT_PARTITIONED:
                    return readFlatPartitionedTable(sourceURI, readInstructions);
                case KV_PARTITIONED:
                    return readKeyValuePartitionedTable(sourceURI, readInstructions, null);
                case METADATA_PARTITIONED:
                    return readPartitionedTableWithMetadata(sourceURI, readInstructions, null);
            }
        }
        if (isParquetFile) {
            return readSingleFileTable(sourceURI, readInstructions);
        }
        if (isMetadataFile) {
            return readPartitionedTableWithMetadata(sourceURI, readInstructions, null);
        }
        return readPartitionedTableDirectory(sourceURI, readInstructions);
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

    private static String minusParquetSuffix(@NotNull final String s) {
        if (s.endsWith(PARQUET_FILE_EXTENSION)) {
            return s.substring(0, s.length() - PARQUET_FILE_EXTENSION.length());
        }
        return s;
    }

    /**
     * Get the name of the file from the URI.
     */
    private static String getFileName(@NotNull final URI uri) {
        final String path = uri.getPath();
        final int lastSlash = path.lastIndexOf(URI_SEPARATOR_CHAR);
        if (lastSlash == path.length() - 1) {
            throw new IllegalArgumentException("Directory URIs are not supported, found" + uri);
        }
        return lastSlash == -1 ? path : path.substring(lastSlash + 1);
    }

    /**
     * Generates the index file path relative to the table destination file path.
     *
     * @param destFileName Destination name for the main table containing these indexing columns
     * @param columnNames Array of indexing column names
     *
     * @return The relative index file path. For example, for table with destination {@code "table.parquet"} and
     *         indexing column {@code "IndexingColName"}, the method will return
     *         {@code ".dh_metadata/indexes/IndexingColName/index_IndexingColName_table.parquet"} on unix systems.
     */
    @VisibleForTesting
    static String getRelativeIndexFilePath(@NotNull final String destFileName, @NotNull final String... columnNames) {
        final String columns = String.join(",", columnNames);
        return String.format(".dh_metadata%sindexes%s%s%sindex_%s_%s", File.separator, File.separator, columns,
                File.separator, columns, destFileName);
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
     * Helper function for building index column info for writing and deleting any backup index column files
     *
     * @param indexColumns Names of index columns, stored as String list for each index
     * @param parquetColumnNameArr Names of index columns for the parquet file, stored as String[] for each index
     * @param dest The destination URI for the main table containing these index columns
     * @param channelProvider The channel provider to use for creating channels to the index files
     */
    private static List<ParquetTableWriter.IndexWritingInfo> indexInfoBuilderHelper(
            @NotNull final Collection<List<String>> indexColumns,
            @NotNull final String[][] parquetColumnNameArr,
            @NotNull final URI dest,
            @NotNull final SeekableChannelsProvider channelProvider) throws IOException {
        Require.eq(indexColumns.size(), "indexColumns.size", parquetColumnNameArr.length,
                "parquetColumnNameArr.length");
        final int numIndexes = indexColumns.size();
        final List<ParquetTableWriter.IndexWritingInfo> indexInfoList = new ArrayList<>(numIndexes);
        int gci = 0;
        final String destFileName = getFileName(dest);
        for (final List<String> indexColumnNames : indexColumns) {
            final String[] parquetColumnNames = parquetColumnNameArr[gci];
            final String indexFileRelativePath = getRelativeIndexFilePath(destFileName, parquetColumnNames);
            final URI indexFileURI = resolve(dest, indexFileRelativePath);
            final CompletableOutputStream indexFileOutputStream =
                    channelProvider.getOutputStream(indexFileURI, PARQUET_OUTPUT_BUFFER_SIZE);
            final ParquetTableWriter.IndexWritingInfo info = new ParquetTableWriter.IndexWritingInfo(
                    indexColumnNames,
                    parquetColumnNames,
                    indexFileURI,
                    indexFileOutputStream);
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
     * @param destinationDir The path or URI to destination root directory to store partitioned data in nested format.
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
     * @param destinationDir The path or URI to destination root directory to store partitioned data in nested format.
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
     * @param destinationRoot The path or URI to destination root directory to store partitioned data in nested format
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
        final Collection<URI> destinations = new ArrayList<>();
        try (final CloseableIterator<ObjectVector<? extends Table>> constituentIterator =
                withGroupConstituents.objectColumnIterator(partitionedTable.constituentColumnName())) {
            int row = 0;
            final URI destinationDir = convertToURI(destinationRoot, true);
            while (constituentIterator.hasNext()) {
                final ObjectVector<? extends Table> constituentVector = constituentIterator.next();
                final List<String> partitionStrings = partitionStringsList.get(row);
                final String relativePath = concatenatePartitions(partitionStrings);
                final URI partitionDir = resolve(destinationDir, relativePath);
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
                    destinations.add(resolve(partitionDir, filename));
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
                    destinations.toArray(URI[]::new), indexColumns, partitioningColumnsSchema,
                    convertToURI(destinationRoot, true), computedCache);
            if (dataIndexes != null) {
                dataIndexes.clear();
            }
        }
    }

    private static String concatenatePartitions(final List<String> partitions) {
        final StringBuilder builder = new StringBuilder();
        for (final String partition : partitions) {
            builder.append(partition).append(File.separator);
        }
        return builder.toString();
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
            @NotNull final URI[] destinations,
            @NotNull final Collection<List<String>> indexColumns,
            @Nullable final MessageType partitioningColumnsSchema,
            @Nullable final URI metadataRootDir,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache) {
        Require.eq(sources.length, "sources.length", destinations.length, "destinations.length");
        if (writeInstructions.getFileLayout().isPresent()) {
            throw new UnsupportedOperationException("File layout is not supported for writing parquet files, use the " +
                    "appropriate API");
        }
        if (definition.numColumns() == 0) {
            throw new TableDataException("Cannot write a parquet table with zero columns");
        }
        // Assuming all destination URIs have the same scheme, and will use the same channels provider instance
        final SeekableChannelsProvider channelsProvider = SeekableChannelsProviderLoader.getInstance()
                .load(destinations[0].getScheme(), writeInstructions.getSpecialInstructions());

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

        // List of output streams created, to rollback in case of exceptions
        final List<CompletableOutputStream> outputStreams = new ArrayList<>(destinations.length);
        try (final SafeCloseable ignored = () -> SafeCloseable.closeAll(outputStreams.stream())) {
            try {
                if (indexColumns.isEmpty()) {
                    // Write the tables without any index info
                    for (int tableIdx = 0; tableIdx < sources.length; tableIdx++) {
                        final Table source = sources[tableIdx];
                        final URI tableDestination = destinations[tableIdx];
                        final CompletableOutputStream outputStream = channelsProvider.getOutputStream(
                                tableDestination, PARQUET_OUTPUT_BUFFER_SIZE);
                        outputStreams.add(outputStream);
                        ParquetTableWriter.write(source, definition, writeInstructions, tableDestination, outputStream,
                                Collections.emptyMap(), (List<ParquetTableWriter.IndexWritingInfo>) null,
                                metadataFileWriter, computedCache);
                    }
                } else {
                    // Shared parquet column names across all tables
                    final String[][] parquetColumnNameArr = indexColumns.stream()
                            .map((Collection<String> columns) -> columns.stream()
                                    .map(writeInstructions::getParquetColumnNameFromColumnNameOrDefault)
                                    .toArray(String[]::new))
                            .toArray(String[][]::new);

                    for (int tableIdx = 0; tableIdx < sources.length; tableIdx++) {
                        final URI tableDestination = destinations[tableIdx];
                        final List<ParquetTableWriter.IndexWritingInfo> indexInfoList =
                                indexInfoBuilderHelper(indexColumns, parquetColumnNameArr, tableDestination,
                                        channelsProvider);
                        final CompletableOutputStream outputStream = channelsProvider.getOutputStream(
                                destinations[tableIdx], PARQUET_OUTPUT_BUFFER_SIZE);
                        outputStreams.add(outputStream);
                        for (final ParquetTableWriter.IndexWritingInfo info : indexInfoList) {
                            outputStreams.add(info.destOutputStream);
                        }
                        final Table source = sources[tableIdx];
                        ParquetTableWriter.write(source, definition, writeInstructions, tableDestination, outputStream,
                                Collections.emptyMap(), indexInfoList, metadataFileWriter, computedCache);
                    }
                }

                if (writeInstructions.generateMetadataFiles()) {
                    final URI metadataDest = metadataRootDir.resolve(METADATA_FILE_NAME);
                    final CompletableOutputStream metadataOutputStream = channelsProvider.getOutputStream(
                            metadataDest, PARQUET_OUTPUT_BUFFER_SIZE);
                    outputStreams.add(metadataOutputStream);
                    final URI commonMetadataDest = metadataRootDir.resolve(COMMON_METADATA_FILE_NAME);
                    final CompletableOutputStream commonMetadataOutputStream = channelsProvider.getOutputStream(
                            commonMetadataDest, PARQUET_OUTPUT_BUFFER_SIZE);
                    outputStreams.add(commonMetadataOutputStream);
                    metadataFileWriter.writeMetadataFiles(metadataOutputStream, commonMetadataOutputStream);
                }

                // Commit all the writes to underlying file system, to detect any exceptions early before closing
                for (final CompletableOutputStream outputStream : outputStreams) {
                    outputStream.complete();
                }
            } catch (final Exception e) {
                // Try to rollback all the output streams in reverse order to undo any writes
                for (int idx = outputStreams.size() - 1; idx >= 0; idx--) {
                    try {
                        outputStreams.get(idx).rollback();
                    } catch (IOException e1) {
                        log.error().append("Error in rolling back output stream ").append(e1).endl();
                    }
                }
                throw new UncheckedDeephavenException("Error writing parquet tables", e);
            }
        }
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
        final URI[] destinationUris = new URI[destinations.length];
        String firstScheme = null;
        for (int idx = 0; idx < destinations.length; idx++) {
            if (!destinations[idx].endsWith(PARQUET_FILE_EXTENSION)) {
                throw new IllegalArgumentException(
                        String.format("Destination %s does not end in %s extension", destinations[idx],
                                PARQUET_FILE_EXTENSION));
            }
            destinationUris[idx] = convertToURI(destinations[idx], false);
            if (idx == 0) {
                firstScheme = destinationUris[0].getScheme();
            } else if (!firstScheme.equals(destinationUris[idx].getScheme())) {
                throw new IllegalArgumentException("All destination URIs must have the same scheme, expected " +
                        firstScheme + " found " + destinationUris[idx].getScheme());
            }
        }
        final URI metadataRootDir;
        if (writeInstructions.generateMetadataFiles()) {
            // We insist on writing the metadata file in the same directory as the destination files, thus all
            // destination files should be in the same directory.
            final URI firstDestinationDir = destinationUris[0].resolve(".");
            for (int i = 1; i < destinations.length; i++) {
                final URI destinationDir = destinationUris[i].resolve(".");
                if (!firstDestinationDir.equals(destinationDir)) {
                    throw new IllegalArgumentException("All destination files must be in the same directory for " +
                            " generating metadata files, found " + firstDestinationDir + " and " + destinationDir);
                }
            }
            metadataRootDir = firstDestinationDir;
        } else {
            metadataRootDir = null;
        }
        final Collection<List<String>> indexColumns =
                writeInstructions.getIndexColumns().orElseGet(() -> indexedColumnNames(sources));
        final Map<String, Map<ParquetCacheTags, Object>> computedCache =
                buildComputedCache(() -> PartitionedTableFactory.ofTables(definition, sources).merge(), definition);
        // We do not have any additional schema for partitioning columns in this case. Schema for all columns will be
        // generated at the time of writing the parquet files and merged to generate the metadata files.
        writeTablesImpl(sources, definition, writeInstructions, destinationUris, indexColumns, null, metadataRootDir,
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
        final TableDefinition tableDefinition = ParquetInstructions.ensureDefinition(readInstructions);
        verifyFileLayout(readInstructions, ParquetFileLayout.SINGLE_FILE);
        final TableLocationProvider locationProvider = new PollingTableLocationProvider<>(
                StandaloneTableKey.getInstance(),
                new KnownLocationKeyFinder<>(tableLocationKey),
                new ParquetTableLocationFactory(readInstructions),
                null,
                TableUpdateMode.STATIC, // exactly one location here
                TableUpdateMode.STATIC); // parquet files are static
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
            useInstructions = infer(inferenceKeys, readInstructions);
            definition = useInstructions.getTableDefinition().orElseThrow();
            // In the case of a static output table, we can re-use the already fetched inference keys
            useLocationKeyFinder = useInstructions.isRefreshing() ? locationKeyFinder : inferenceKeys;
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
                        refreshService,
                        // If refreshing, new locations can be discovered but they will be appended
                        // to the locations list
                        useInstructions.isRefreshing()
                                ? TableUpdateMode.APPEND_ONLY
                                : TableUpdateMode.STATIC,
                        TableUpdateMode.STATIC // parquet files are static
                ),
                updateSourceRegistrar);
    }

    /**
     * Infers additional information regarding the parquet file(s) based on the inferenceKeys and returns a potentially
     * updated parquet instructions. If the incoming {@code readInstructions} does not have a {@link TableDefinition},
     * the returned instructions will have an inferred {@link TableDefinition}.
     */
    private static ParquetInstructions infer(
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
        return ensureTableDefinition(schemaInfo.getSecond(), TableDefinition.of(allColumns), true);
    }

    private static KnownLocationKeyFinder<ParquetTableLocationKey> toKnownKeys(
            TableLocationKeyFinder<ParquetTableLocationKey> keyFinder) {
        return keyFinder instanceof KnownLocationKeyFinder
                ? (KnownLocationKeyFinder<ParquetTableLocationKey>) keyFinder
                : KnownLocationKeyFinder.copyFrom(keyFinder, Comparator.naturalOrder());
    }

    private static Table readPartitionedTableDirectory(
            @NotNull final URI tableRootDirectory,
            @NotNull final ParquetInstructions readInstructions) {
        // Check if the directory has a metadata file
        final URI metadataFileURI = tableRootDirectory.resolve(METADATA_FILE_NAME);
        final SeekableChannelsProvider channelsProvider =
                SeekableChannelsProviderLoader.getInstance().load(tableRootDirectory.getScheme(),
                        readInstructions.getSpecialInstructions());
        if (channelsProvider.exists(metadataFileURI)) {
            return readPartitionedTableWithMetadata(metadataFileURI, readInstructions, channelsProvider);
        }
        // Both flat partitioned and key-value partitioned data can be read under key-value partitioned layout
        return readKeyValuePartitionedTable(tableRootDirectory, readInstructions, channelsProvider);
    }

    /**
     * Creates a partitioned table via the metadata parquet files from the root {@code directory}, inferring the table
     * definition from those files.
     *
     * @param sourceURI the path or URI for the directory to search for .parquet files, the
     *        {@value ParquetUtils#METADATA_FILE_NAME} file or the {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file.
     *        Note that the {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file must be present in the same directory.
     * @param readInstructions the instructions for customizations while reading
     * @param channelsProvider the provider for creating seekable channels. If null, a new provider will be created and
     *        used for all channels created while reading the table.
     */
    private static Table readPartitionedTableWithMetadata(
            @NotNull final URI sourceURI,
            @NotNull final ParquetInstructions readInstructions,
            @Nullable final SeekableChannelsProvider channelsProvider) {
        verifyFileLayout(readInstructions, ParquetFileLayout.METADATA_PARTITIONED);
        final ParquetMetadataFileLayout layout =
                ParquetMetadataFileLayout.create(sourceURI, readInstructions, channelsProvider);
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
     * @param channelsProvider the provider for creating seekable channels. If null, a new provider will be created and
     *        used for all channels created while reading the table.
     * @return the table
     */
    private static Table readKeyValuePartitionedTable(
            @NotNull final URI directoryUri,
            @NotNull final ParquetInstructions readInstructions,
            @Nullable final SeekableChannelsProvider channelsProvider) {
        verifyFileLayout(readInstructions, ParquetFileLayout.KV_PARTITIONED);
        if (readInstructions.getTableDefinition().isEmpty()) {
            return readTable(ParquetKeyValuePartitionedLayout.create(directoryUri,
                    MAX_PARTITIONING_LEVELS_INFERENCE, readInstructions, channelsProvider), readInstructions);
        }
        final TableDefinition tableDefinition = readInstructions.getTableDefinition().get();
        return readTable(ParquetKeyValuePartitionedLayout.create(directoryUri, tableDefinition,
                readInstructions, channelsProvider), readInstructions);
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
        return readTable(inferenceKeys.getFirstKey().orElseThrow(), infer(inferenceKeys, readInstructions));
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
