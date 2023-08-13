/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.ClassUtil;
import io.deephaven.base.FileUtils;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.engine.util.TableTools;
import io.deephaven.vector.*;
import io.deephaven.stringset.StringSet;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.SimpleSourceTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.*;
import io.deephaven.parquet.table.layout.ParquetFlatPartitionedLayout;
import io.deephaven.parquet.table.layout.ParquetKeyValuePartitionedLayout;
import io.deephaven.parquet.table.layout.ParquetMetadataFileLayout;
import io.deephaven.parquet.table.layout.ParquetSingleFileLayout;
import io.deephaven.parquet.table.location.ParquetTableLocationFactory;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.parquet.table.util.TrackedSeekableChannelsProvider;
import io.deephaven.parquet.table.metadata.ColumnTypeInfo;
import io.deephaven.api.util.NameValidator;
import io.deephaven.util.SimpleTypeMap;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.parquet.base.ParquetFileReader;
import io.deephaven.parquet.base.tempfix.ParquetMetadataConverter;
import io.deephaven.parquet.base.util.CachedChannelProvider;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Function;

import static io.deephaven.parquet.table.ParquetTableWriter.PARQUET_FILE_EXTENSION;
import static io.deephaven.util.type.TypeUtils.getUnboxedTypeIfBoxed;

/**
 * Tools for managing and manipulating tables on disk in parquet format.
 */
@SuppressWarnings("WeakerAccess")
public class ParquetTools {

    private ParquetTools() {}

    private static final Logger log = LoggerFactory.getLogger(ParquetTools.class);

    /**
     * Reads in a table from a single parquet, metadata file, or directory with recognized layout.
     *
     * @param sourceFilePath The file or directory to examine
     * @return table
     * @see ParquetSingleFileLayout
     * @see ParquetMetadataFileLayout
     * @see ParquetKeyValuePartitionedLayout
     * @see ParquetFlatPartitionedLayout
     */
    public static Table readTable(@NotNull final String sourceFilePath) {
        return readTableInternal(new File(sourceFilePath), ParquetInstructions.EMPTY);
    }

    /**
     * Reads in a table from a single parquet, metadata file, or directory with recognized layout.
     *
     * @param sourceFilePath The file or directory to examine
     * @param readInstructions Instructions for customizations while reading
     * @return table
     * @see ParquetSingleFileLayout
     * @see ParquetMetadataFileLayout
     * @see ParquetKeyValuePartitionedLayout
     * @see ParquetFlatPartitionedLayout
     */
    public static Table readTable(
            @NotNull final String sourceFilePath,
            @NotNull final ParquetInstructions readInstructions) {
        return readTableInternal(new File(sourceFilePath), readInstructions);
    }

    /**
     * Reads in a table from a single parquet, metadata file, or directory with recognized layout.
     *
     * @param sourceFile The file or directory to examine
     * @return table
     * @see ParquetSingleFileLayout
     * @see ParquetMetadataFileLayout
     * @see ParquetKeyValuePartitionedLayout
     * @see ParquetFlatPartitionedLayout
     */
    public static Table readTable(@NotNull final File sourceFile) {
        return readTableInternal(sourceFile, ParquetInstructions.EMPTY);
    }

    /**
     * Reads in a table from a single parquet, metadata file, or directory with recognized layout.
     *
     * @param sourceFile The file or directory to examine
     * @param readInstructions Instructions for customizations while reading
     * @return table
     * @see ParquetSingleFileLayout
     * @see ParquetMetadataFileLayout
     * @see ParquetKeyValuePartitionedLayout
     * @see ParquetFlatPartitionedLayout
     */
    public static Table readTable(
            @NotNull final File sourceFile,
            @NotNull final ParquetInstructions readInstructions) {
        return readTableInternal(sourceFile, readInstructions);
    }

    /**
     * Write a table to a file.
     *
     * @param sourceTable source table
     * @param destPath destination file path; the file name should end in ".parquet" extension If the path includes
     *        non-existing directories they are created If there is an error any intermediate directories previously
     *        created are removed; note this makes this method unsafe for concurrent use
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final String destPath) {
        writeTable(sourceTable, new File(destPath), sourceTable.getDefinition(), ParquetInstructions.EMPTY);
    }

    /**
     * Write a table to a file.
     *
     * @param sourceTable source table
     * @param destFile destination file; the file name should end in ".parquet" extension If the path includes
     *        non-existing directories they are created
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final File destFile) {
        writeTable(sourceTable, destFile, sourceTable.getDefinition(), ParquetInstructions.EMPTY);
    }

    /**
     * Write a table to a file.
     *
     * @param sourceTable source table
     * @param destFile destination file; its path must end in ".parquet". Any non existing directories in the path are
     *        created If there is an error any intermediate directories previously created are removed; note this makes
     *        this method unsafe for concurrent use
     * @param definition table definition to use (instead of the one implied by the table itself)
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final File destFile,
            @NotNull final TableDefinition definition) {
        writeTable(sourceTable, destFile, definition, ParquetInstructions.EMPTY);
    }

    /**
     * Write a table to a file.
     *
     * @param sourceTable source table
     * @param destFile destination file; its path must end in ".parquet". Any non existing directories in the path are
     *        created If there is an error any intermediate directories previously created are removed; note this makes
     *        this method unsafe for concurrent use
     * @param writeInstructions instructions for customizations while writing
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final File destFile,
            @NotNull final ParquetInstructions writeInstructions) {
        writeTable(sourceTable, destFile, sourceTable.getDefinition(), writeInstructions);
    }

    /**
     * Write a table to a file.
     *
     * @param sourceTable source table
     * @param destPath destination path; it must end in ".parquet". Any non existing directories in the path are created
     *        If there is an error any intermediate directories previously created are removed; note this makes this
     *        method unsafe for concurrent use
     * @param definition table definition to use (instead of the one implied by the table itself)
     * @param writeInstructions instructions for customizations while writing
     */
    public static void writeTable(@NotNull final Table sourceTable,
            @NotNull final String destPath,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions) {
        writeTable(sourceTable, new File(destPath), definition, writeInstructions);
    }

    /**
     * Write a table to a file.
     *
     * @param sourceTable source table
     * @param destFile destination file; its path must end in ".parquet". Any non-existing directories in the path are
     *        created If there is an error any intermediate directories previously created are removed; note this makes
     *        this method unsafe for concurrent use
     * @param definition table definition to use (instead of the one implied by the table itself)
     * @param writeInstructions instructions for customizations while writing
     */
    public static void writeTable(@NotNull final Table sourceTable,
            @NotNull final File destFile,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions) {
        writeTables(new Table[] {sourceTable}, definition, new File[] {destFile}, writeInstructions);
    }

    private static File getShadowFile(File destFile) {
        return new File(destFile.getParent(), ".NEW_" + destFile.getName());
    }

    @VisibleForTesting
    static File getBackupFile(File destFile) {
        return new File(destFile.getParent(), ".OLD_" + destFile.getName());
    }

    private static String minusParquetSuffix(@NotNull final String s) {
        if (s.endsWith(PARQUET_FILE_EXTENSION)) {
            return s.substring(0, s.length() - PARQUET_FILE_EXTENSION.length());
        }
        return s;
    }

    public static Function<String, String> defaultGroupingFileName(@NotNull final String path) {
        final String prefix = minusParquetSuffix(path);
        return columnName -> prefix + "_" + columnName + "_grouping.parquet";
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
     * Roll back any changes made in the {@link #installShadowFile} in a best effort manner
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
     * @param destination The destination file
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
     * Helper function for building grouping column info for writing and deleting any backup grouping column files
     *
     * @param groupingColumnNames Names of grouping columns
     * @param parquetColumnNames Names of grouping columns for the parquet file
     * @param destFile The destination path for the main table containing these grouping columns
     */
    private static Map<String, ParquetTableWriter.GroupingColumnWritingInfo> groupingColumnInfoBuilderHelper(
            @NotNull final String[] groupingColumnNames,
            @NotNull final String[] parquetColumnNames,
            @NotNull final File destFile) {
        Require.eq(groupingColumnNames.length, "groupingColumnNames.length", parquetColumnNames.length,
                "parquetColumnNames.length");
        final Map<String, ParquetTableWriter.GroupingColumnWritingInfo> gcwim = new HashMap<>();
        for (int gci = 0; gci < groupingColumnNames.length; gci++) {
            final String groupingColumnName = groupingColumnNames[gci];
            final String parquetColumnName = parquetColumnNames[gci];
            final String groupingFilePath = defaultGroupingFileName(destFile.getPath()).apply(parquetColumnName);
            final File groupingFile = new File(groupingFilePath);
            deleteBackupFile(groupingFile);
            final File shadowGroupingFile = getShadowFile(groupingFile);
            gcwim.put(groupingColumnName, new ParquetTableWriter.GroupingColumnWritingInfo(parquetColumnName,
                    groupingFile, shadowGroupingFile));
        }
        return gcwim;
    }

    /**
     * Writes tables to disk in parquet format to a supplied set of destinations. If you specify grouping columns, there
     * must already be grouping information for those columns in the sources. This can be accomplished with
     * {@code .groupBy(<grouping columns>).ungroup()} or {@code .sort(<grouping column>)}.
     *
     * @param sources The tables to write
     * @param definition The common schema for all the tables to write
     * @param writeInstructions Write instructions for customizations while writing
     * @param destinations The destinations paths. Any non-existing directories in the paths provided are created. If
     *        there is an error any intermediate directories previously created are removed; note this makes this method
     *        unsafe for concurrent use
     * @param groupingColumns List of columns the tables are grouped by (the write operation will store the grouping
     *        info)
     */
    public static void writeParquetTables(@NotNull final Table[] sources,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final File[] destinations,
            @NotNull final String[] groupingColumns) {
        Require.eq(sources.length, "sources.length", destinations.length, "destinations.length");
        if (definition.numColumns() == 0) {
            throw new TableDataException("Cannot write a parquet table with zero columns");
        }
        Arrays.stream(destinations).forEach(ParquetTools::deleteBackupFile);

        // Write tables at temporary shadow file paths in the same directory to prevent overwriting any existing files
        final File[] shadowDestFiles =
                Arrays.stream(destinations)
                        .map(ParquetTools::getShadowFile)
                        .toArray(File[]::new);
        final File[] firstCreatedDirs =
                Arrays.stream(shadowDestFiles)
                        .map(ParquetTools::prepareDestinationFileLocation)
                        .toArray(File[]::new);

        // List of shadow files, to clean up in case of exceptions
        final List<File> shadowFiles = new ArrayList<>();
        // List of all destination files (including grouping files), to roll back in case of exceptions
        final List<File> destFiles = new ArrayList<>();
        try {
            final List<Map<String, ParquetTableWriter.GroupingColumnWritingInfo>> groupingColumnWritingInfoMaps;
            if (groupingColumns.length == 0) {
                // Write the tables without any grouping info
                groupingColumnWritingInfoMaps = null;
                for (int tableIdx = 0; tableIdx < sources.length; tableIdx++) {
                    shadowFiles.add(shadowDestFiles[tableIdx]);
                    final Table source = sources[tableIdx];
                    ParquetTableWriter.write(source, definition, writeInstructions, shadowDestFiles[tableIdx].getPath(),
                            Collections.emptyMap(), (Map<String, ParquetTableWriter.GroupingColumnWritingInfo>) null);
                }
            } else {
                // Create grouping info for each table and write the table and grouping files to shadow path
                groupingColumnWritingInfoMaps = new ArrayList<>(sources.length);

                // Shared parquet column names across all tables
                final String[] parquetColumnNames = Arrays.stream(groupingColumns)
                        .map(writeInstructions::getParquetColumnNameFromColumnNameOrDefault)
                        .toArray(String[]::new);

                for (int tableIdx = 0; tableIdx < sources.length; tableIdx++) {
                    final File tableDestination = destinations[tableIdx];
                    final Map<String, ParquetTableWriter.GroupingColumnWritingInfo> groupingColumnWritingInfoMap =
                            groupingColumnInfoBuilderHelper(groupingColumns, parquetColumnNames, tableDestination);
                    groupingColumnWritingInfoMaps.add(groupingColumnWritingInfoMap);

                    shadowFiles.add(shadowDestFiles[tableIdx]);
                    groupingColumnWritingInfoMap.values().forEach(gcwi -> shadowFiles.add(gcwi.destFile));

                    final Table sourceTable = sources[tableIdx];
                    ParquetTableWriter.write(sourceTable, definition, writeInstructions,
                            shadowDestFiles[tableIdx].getPath(),
                            Collections.emptyMap(), groupingColumnWritingInfoMap);
                }
            }

            // Write to shadow files was successful
            for (int tableIdx = 0; tableIdx < sources.length; tableIdx++) {
                destFiles.add(destinations[tableIdx]);
                installShadowFile(destinations[tableIdx], shadowDestFiles[tableIdx]);
                if (groupingColumnWritingInfoMaps != null) {
                    final Map<String, ParquetTableWriter.GroupingColumnWritingInfo> gcwim =
                            groupingColumnWritingInfoMaps.get(tableIdx);
                    for (final ParquetTableWriter.GroupingColumnWritingInfo gfwi : gcwim.values()) {
                        final File groupingDestFile = gfwi.metadataFilePath;
                        final File shadowGroupingFile = gfwi.destFile;
                        destFiles.add(groupingDestFile);
                        installShadowFile(groupingDestFile, shadowGroupingFile);
                    }
                }
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
        destFiles.stream().forEach(ParquetTools::deleteBackupFileNoExcept);
    }

    /**
     * Write out tables to disk.
     *
     * @param sources source tables
     * @param definition table definition
     * @param destinations destinations
     */
    public static void writeTables(@NotNull final Table[] sources,
            @NotNull final TableDefinition definition,
            @NotNull final File[] destinations) {
        writeParquetTables(sources, definition, ParquetInstructions.EMPTY, destinations,
                definition.getGroupingColumnNamesArray());
    }

    /**
     * Write out tables to disk.
     *
     * @param sources source tables
     * @param definition table definition
     * @param destinations destinations
     * @param writeInstructions instructions for customizations while writing
     */
    public static void writeTables(@NotNull final Table[] sources,
            @NotNull final TableDefinition definition,
            @NotNull final File[] destinations,
            @NotNull final ParquetInstructions writeInstructions) {
        writeParquetTables(sources, definition, writeInstructions, destinations,
                definition.getGroupingColumnNamesArray());
    }

    /**
     * Deletes a table on disk.
     *
     * @param path path to delete
     */
    @VisibleForTesting
    public static void deleteTable(File path) {
        FileUtils.deleteRecursivelyOnNFS(path);
    }

    /**
     * This method attempts to "do the right thing." It examines the source to determine if it's a single parquet file,
     * a metadata file, or a directory. If it's a directory, it additionally tries to guess the layout to use. Unless a
     * metadata file is supplied or discovered in the directory, the first found parquet file will be used to infer
     * schema.
     *
     * @param source The source file or directory
     * @param instructions Instructions for reading
     * @return A {@link Table}
     */
    private static Table readTableInternal(
            @NotNull final File source,
            @NotNull final ParquetInstructions instructions) {
        final Path sourcePath = source.toPath();
        if (!Files.exists(sourcePath)) {
            throw new TableDataException("Source file " + source + " does not exist");
        }
        final String sourceFileName = sourcePath.getFileName().toString();
        final BasicFileAttributes sourceAttr = readAttributes(sourcePath);
        if (sourceAttr.isRegularFile()) {
            if (sourceFileName.endsWith(PARQUET_FILE_EXTENSION)) {
                if (instructions.isRefreshing()) {
                    throw new IllegalArgumentException("Unable to have a refreshing single parquet file");
                }
                final ParquetTableLocationKey tableLocationKey = new ParquetTableLocationKey(source, 0, null);
                final Pair<List<ColumnDefinition<?>>, ParquetInstructions> schemaInfo = convertSchema(
                        tableLocationKey.getFileReader().getSchema(),
                        tableLocationKey.getMetadata().getFileMetaData().getKeyValueMetaData(),
                        instructions);
                return readSingleFileTable(tableLocationKey, schemaInfo.getSecond(),
                        TableDefinition.of(schemaInfo.getFirst()));
            }
            if (sourceFileName.equals(ParquetMetadataFileLayout.METADATA_FILE_NAME)) {
                return readPartitionedTableWithMetadata(source.getParentFile(), instructions);
            }
            if (sourceFileName.equals(ParquetMetadataFileLayout.COMMON_METADATA_FILE_NAME)) {
                return readPartitionedTableWithMetadata(source.getParentFile(), instructions);
            }
            throw new TableDataException(
                    "Source file " + source + " does not appear to be a parquet file or metadata file");
        }
        if (sourceAttr.isDirectory()) {
            final Path metadataPath = sourcePath.resolve(ParquetMetadataFileLayout.METADATA_FILE_NAME);
            if (Files.exists(metadataPath)) {
                return readPartitionedTableWithMetadata(source, instructions);
            }
            final Path firstEntryPath;
            try (final DirectoryStream<Path> sourceStream = Files.newDirectoryStream(sourcePath)) {
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
                return readPartitionedTableInferSchema(new ParquetKeyValuePartitionedLayout(source, 32), instructions);
            }
            if (firstEntryAttr.isRegularFile() && firstEntryFileName.endsWith(PARQUET_FILE_EXTENSION)) {
                return readPartitionedTableInferSchema(new ParquetFlatPartitionedLayout(source), instructions);
            }
            throw new TableDataException("No recognized Parquet table layout found in " + source);
        }
        throw new TableDataException("Source " + source + " is neither a directory nor a regular file");
    }

    private static BasicFileAttributes readAttributes(@NotNull final Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            throw new TableDataException("Failed to read " + path + " file attributes", e);
        }
    }

    /**
     * Reads in a table from a single parquet file using the provided table definition.
     *
     * @param tableLocationKey The {@link ParquetTableLocationKey location keys} to include
     * @param readInstructions Instructions for customizations while reading
     * @param tableDefinition The table's {@link TableDefinition definition}
     * @return The table
     */
    public static Table readSingleFileTable(
            @NotNull final ParquetTableLocationKey tableLocationKey,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final TableDefinition tableDefinition) {
        final TableLocationProvider locationProvider = new PollingTableLocationProvider<>(
                StandaloneTableKey.getInstance(),
                new KnownLocationKeyFinder<>(tableLocationKey),
                new ParquetTableLocationFactory(readInstructions),
                null);
        return new SimpleSourceTable(tableDefinition.getWritable(),
                "Read single parquet file from " + tableLocationKey.getFile(),
                RegionedTableComponentFactoryImpl.INSTANCE, locationProvider, null);
    }

    /**
     * Reads in a table from files discovered with {@code locationKeyFinder} using the provided table definition.
     *
     * @param locationKeyFinder The source of {@link ParquetTableLocationKey location keys} to include
     * @param readInstructions Instructions for customizations while reading
     * @param tableDefinition The table's {@link TableDefinition definition}
     * @return The table
     */
    public static Table readPartitionedTable(
            @NotNull final TableLocationKeyFinder<ParquetTableLocationKey> locationKeyFinder,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final TableDefinition tableDefinition) {
        final TableLocationProvider locationProvider = new PollingTableLocationProvider<>(
                StandaloneTableKey.getInstance(),
                locationKeyFinder,
                new ParquetTableLocationFactory(readInstructions),
                readInstructions.isRefreshing() ? TableDataRefreshService.getSharedRefreshService() : null);
        return new PartitionAwareSourceTable(
                tableDefinition,
                readInstructions.isRefreshing()
                        ? "Read refreshing parquet files with " + locationKeyFinder
                        : "Read multiple parquet files with " + locationKeyFinder,
                RegionedTableComponentFactoryImpl.INSTANCE,
                locationProvider,
                readInstructions.isRefreshing() ? ExecutionContext.getContext().getUpdateGraph() : null);
    }

    /**
     * Reads in a table from files discovered with {@code locationKeyFinder} using a definition built from the first
     * location found, which must have non-null partition values for all partition keys.
     *
     * @param locationKeyFinder The source of {@link ParquetTableLocationKey location keys} to include
     * @param readInstructions Instructions for customizations while reading
     * @return The table
     */
    public static Table readPartitionedTableInferSchema(
            @NotNull final TableLocationKeyFinder<ParquetTableLocationKey> locationKeyFinder,
            @NotNull final ParquetInstructions readInstructions) {
        final RecordingLocationKeyFinder<ParquetTableLocationKey> initialKeys = new RecordingLocationKeyFinder<>();
        locationKeyFinder.findKeys(initialKeys);
        final List<ParquetTableLocationKey> foundKeys = initialKeys.getRecordedKeys();
        if (foundKeys.isEmpty()) {
            if (readInstructions.isRefreshing()) {
                throw new IllegalArgumentException(
                        "Unable to infer schema for a refreshing partitioned parquet table when there are no initial parquet files");
            }
            return TableTools.emptyTable(0);
        }
        // TODO (https://github.com/deephaven/deephaven-core/issues/877): Support schema merge when discovering multiple
        // parquet files
        final ParquetTableLocationKey firstKey = foundKeys.get(0);
        final Pair<List<ColumnDefinition<?>>, ParquetInstructions> schemaInfo = convertSchema(
                firstKey.getFileReader().getSchema(),
                firstKey.getMetadata().getFileMetaData().getKeyValueMetaData(),
                readInstructions);
        final List<ColumnDefinition<?>> allColumns =
                new ArrayList<>(firstKey.getPartitionKeys().size() + schemaInfo.getFirst().size());
        for (final String partitionKey : firstKey.getPartitionKeys()) {
            final Comparable<?> partitionValue = firstKey.getPartitionValue(partitionKey);
            if (partitionValue == null) {
                throw new IllegalArgumentException("First location key " + firstKey
                        + " has null partition value at partition key " + partitionKey);
            }
            allColumns.add(ColumnDefinition.fromGenericType(partitionKey,
                    getUnboxedTypeIfBoxed(partitionValue.getClass()), null, ColumnDefinition.ColumnType.Partitioning));
        }
        allColumns.addAll(schemaInfo.getFirst());
        return readPartitionedTable(readInstructions.isRefreshing() ? locationKeyFinder : initialKeys,
                schemaInfo.getSecond(), TableDefinition.of(allColumns));
    }

    /**
     * Reads in a table using metadata files found in the supplied directory.
     *
     * @param directory The source of {@link ParquetTableLocationKey location keys} to include
     * @param readInstructions Instructions for customizations while reading
     * @return The table
     */
    public static Table readPartitionedTableWithMetadata(
            @NotNull final File directory,
            @NotNull final ParquetInstructions readInstructions) {
        final ParquetMetadataFileLayout layout = new ParquetMetadataFileLayout(directory, readInstructions);
        return readPartitionedTable(layout, layout.getInstructions(), layout.getTableDefinition());
    }

    private static final SimpleTypeMap<Class<?>> DB_ARRAY_TYPE_MAP = SimpleTypeMap.create(
            null, CharVector.class, ByteVector.class, ShortVector.class, IntVector.class, LongVector.class,
            FloatVector.class, DoubleVector.class, ObjectVector.class);

    private static Class<?> loadClass(final String colName, final String desc, final String className) {
        try {
            return ClassUtil.lookupClass(className);
        } catch (ClassNotFoundException e) {
            throw new UncheckedDeephavenException(
                    "Column " + colName + " with " + desc + "=" + className + " that can't be found in classloader");
        }
    }

    private static ParquetSchemaReader.ColumnDefinitionConsumer makeSchemaReaderConsumer(
            final ArrayList<ColumnDefinition<?>> colsOut) {
        return (final ParquetSchemaReader.ParquetMessageDefinition parquetColDef) -> {
            Class<?> baseType;
            if (parquetColDef.baseType == boolean.class) {
                baseType = Boolean.class;
            } else {
                baseType = parquetColDef.baseType;
            }
            ColumnDefinition<?> colDef;
            if (parquetColDef.codecType != null && !parquetColDef.codecType.isEmpty()) {
                final Class<?> componentType =
                        (parquetColDef.codecComponentType != null && !parquetColDef.codecComponentType.isEmpty())
                                ? loadClass(parquetColDef.name, "codecComponentType", parquetColDef.codecComponentType)
                                : null;
                final Class<?> dataType = loadClass(parquetColDef.name, "codecType", parquetColDef.codecType);
                colDef = ColumnDefinition.fromGenericType(parquetColDef.name, dataType, componentType);
            } else if (parquetColDef.dhSpecialType != null) {
                if (parquetColDef.dhSpecialType == ColumnTypeInfo.SpecialType.StringSet) {
                    colDef = ColumnDefinition.fromGenericType(parquetColDef.name, StringSet.class, null);
                } else if (parquetColDef.dhSpecialType == ColumnTypeInfo.SpecialType.Vector) {
                    final Class<?> vectorType = DB_ARRAY_TYPE_MAP.get(baseType);
                    if (vectorType != null) {
                        colDef = ColumnDefinition.fromGenericType(parquetColDef.name, vectorType, baseType);
                    } else {
                        colDef = ColumnDefinition.fromGenericType(parquetColDef.name, ObjectVector.class, baseType);
                    }
                } else {
                    throw new UncheckedDeephavenException("Unhandled dbSpecialType=" + parquetColDef.dhSpecialType);
                }
            } else {
                if (parquetColDef.isArray) {
                    if (baseType == byte.class && parquetColDef.noLogicalType) {
                        colDef = ColumnDefinition.fromGenericType(parquetColDef.name, byte[].class, byte.class);
                    } else {
                        // TODO: ParquetInstruction.loadAsVector
                        final Class<?> componentType = baseType;
                        // On Java 12, replace by: dataType = componentType.arrayType();
                        final Class<?> dataType = java.lang.reflect.Array.newInstance(componentType, 0).getClass();
                        colDef = ColumnDefinition.fromGenericType(parquetColDef.name, dataType, componentType);
                    }
                } else {
                    colDef = ColumnDefinition.fromGenericType(parquetColDef.name, baseType, null);
                }
            }
            if (parquetColDef.isGrouping) {
                colDef = colDef.withGrouping();
            }
            colsOut.add(colDef);
        };
    }

    /**
     * Make a {@link ParquetFileReader} for the supplied {@link File}. Wraps {@link IOException} as
     * {@link TableDataException}.
     *
     * @param parquetFile The {@link File} to read
     * @return The new {@link ParquetFileReader}
     */
    public static ParquetFileReader getParquetFileReader(@NotNull final File parquetFile) {
        try {
            return getParquetFileReaderChecked(parquetFile);
        } catch (IOException e) {
            throw new TableDataException("Failed to create Parquet file reader: " + parquetFile, e);
        }
    }

    /**
     * Make a {@link ParquetFileReader} for the supplied {@link File}.
     *
     * @param parquetFile The {@link File} to read
     * @return The new {@link ParquetFileReader}
     * @throws IOException if an IO exception occurs
     */
    public static ParquetFileReader getParquetFileReaderChecked(@NotNull File parquetFile) throws IOException {
        return new ParquetFileReader(
                parquetFile.getAbsolutePath(),
                new CachedChannelProvider(
                        new TrackedSeekableChannelsProvider(TrackedFileHandleFactory.getInstance()), 1 << 7));
    }

    @VisibleForTesting
    public static Table readParquetSchemaAndTable(
            @NotNull final File source, @NotNull final ParquetInstructions readInstructionsIn,
            MutableObject<ParquetInstructions> instructionsOut) {
        final ParquetTableLocationKey tableLocationKey = new ParquetTableLocationKey(source, 0, null);
        final Pair<List<ColumnDefinition<?>>, ParquetInstructions> schemaInfo = convertSchema(
                tableLocationKey.getFileReader().getSchema(),
                tableLocationKey.getMetadata().getFileMetaData().getKeyValueMetaData(),
                readInstructionsIn);
        final TableDefinition def = TableDefinition.of(schemaInfo.getFirst());
        if (instructionsOut != null) {
            instructionsOut.setValue(schemaInfo.getSecond());
        }
        return readSingleFileTable(tableLocationKey, schemaInfo.getSecond(), def);
    }

    /**
     * Convert schema information from a {@link ParquetMetadata} into {@link ColumnDefinition ColumnDefinitions}.
     *
     * @param schema Parquet schema. DO NOT RELY ON {@link ParquetMetadataConverter} FOR THIS! USE
     *        {@link ParquetFileReader}!
     * @param keyValueMetadata Parquet key-value metadata map
     * @param readInstructionsIn Input conversion {@link ParquetInstructions}
     * @return A {@link Pair} with {@link ColumnDefinition ColumnDefinitions} and adjusted {@link ParquetInstructions}
     */
    public static Pair<List<ColumnDefinition<?>>, ParquetInstructions> convertSchema(
            @NotNull final MessageType schema,
            @NotNull final Map<String, String> keyValueMetadata,
            @NotNull final ParquetInstructions readInstructionsIn) {
        final ArrayList<ColumnDefinition<?>> cols = new ArrayList<>();
        final ParquetSchemaReader.ColumnDefinitionConsumer colConsumer = makeSchemaReaderConsumer(cols);
        return new Pair<>(cols, ParquetSchemaReader.readParquetSchema(
                schema,
                keyValueMetadata,
                readInstructionsIn,
                colConsumer,
                (final String colName, final Set<String> takenNames) -> NameValidator.legalizeColumnName(colName,
                        s -> s.replace(" ", "_"), takenNames)));
    }

    public static final ParquetInstructions LZ4 = ParquetInstructions.builder().setCompressionCodecName("LZ4").build();
    public static final ParquetInstructions LZO = ParquetInstructions.builder().setCompressionCodecName("LZO").build();
    public static final ParquetInstructions GZIP =
            ParquetInstructions.builder().setCompressionCodecName("GZIP").build();
    public static final ParquetInstructions ZSTD =
            ParquetInstructions.builder().setCompressionCodecName("ZSTD").build();
    public static final ParquetInstructions LEGACY = ParquetInstructions.builder().setIsLegacyParquet(true).build();

    public static void setDefaultCompressionCodecName(final String compressionCodecName) {
        ParquetInstructions.setDefaultCompressionCodecName(compressionCodecName);
    }
}
