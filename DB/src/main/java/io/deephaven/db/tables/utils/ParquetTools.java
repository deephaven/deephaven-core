/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.ClassUtil;
import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.dbarrays.*;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.v2.locations.*;
import io.deephaven.db.v2.locations.local.ParquetTableLocationScanner;
import io.deephaven.db.v2.locations.parquet.ParquetTableLocationKey;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.db.v2.parquet.ParquetSchemaReader;
import io.deephaven.db.v2.sources.chunk.util.SimpleTypeMap;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.SimpleSourceTable;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.db.v2.parquet.ParquetTableWriter.PARQUET_FILE_EXTENSION;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Tools for managing and manipulating tables on disk in parquet format.
 */
@SuppressWarnings("WeakerAccess")
public class ParquetTools {

    private ParquetTools() {
    }

    private static final Logger log = LoggerFactory.getLogger(ParquetTools.class);

    /**
     * Reads in a table from a file.
     *
     * @param sourceFilePath table location; the file should exist and end in ".parquet" extension
     * @return table
     */
    public static Table readTable(@NotNull final String sourceFilePath) {
        return readParquetSchemaAndTable(new File(sourceFilePath), ParquetInstructions.EMPTY);
    }

    /**
     * Reads in a table from a file.
     *
     * @param sourceFilePath table location; the file should exist and end in ".parquet" extension
     * @param readInstructions instructions for customizations while reading
     * @return table
     */
    public static Table readTable(
            @NotNull final String sourceFilePath,
            @NotNull final ParquetInstructions readInstructions) {
        return readParquetSchemaAndTable(new File(sourceFilePath), readInstructions);
    }

    /**
     * Reads in a table from a file.
     *
     * @param sourceFile table location; the file should exist and end in ".parquet" extension
     * @return table
     */
    public static Table readTable(@NotNull final File sourceFile) {
        return readParquetSchemaAndTable(sourceFile, ParquetInstructions.EMPTY);
    }

    /**
     * Reads in a table from a file.
     *
     * @param sourceFile table location; the file should exist and end in ".parquet" extension
     * @param readInstructions instructions for customizations while reading
     * @return table
     */
    public static Table readTable(
            @NotNull final File sourceFile,
            @NotNull final ParquetInstructions readInstructions) {
        return readParquetSchemaAndTable(sourceFile, readInstructions);
    }

    /**
     * Reads in a table from a file using the provided table definition.
     *
     * @param sourceFilePath table location; the file should exist and end in ".parquet" extension
     * @param definition table definition
     * @return table
     */
    public static Table readTable(
            @NotNull final String sourceFilePath,
            final TableDefinition definition) {
        return readTableFromSingleParquetFile(new File(sourceFilePath), ParquetInstructions.EMPTY, definition);
    }

    /**
     * Reads in a table from a file, using the provided table definition
     * (instead of the definition implied by the file).
     *
     * @param sourceFilePath table location; the file should exist and end in ".parquet" extension
     * @param definition table definition
     * @param readInstructions instructions for customizations while reading
     * @return table
     */
    public static Table readTable(
            @NotNull final String sourceFilePath,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions readInstructions) {
        return readTableFromSingleParquetFile(new File(sourceFilePath), readInstructions, definition);
    }

    /**
     * Reads in a table from a file, using the provided table definition.
     *
     * @param sourceFile table location; the file should exist and end in ".parquet" extension
     * @param definition table definition
     * @return table
     */
    public static Table readTable(
            @NotNull final File sourceFile,
            @NotNull final TableDefinition definition) {
        return readTableFromSingleParquetFile(sourceFile, ParquetInstructions.EMPTY, definition);
    }

    /**
     * Reads in a table from a file, using the provided table definition.
     *
     * @param sourceFile table location; the file should exist and end in ".parquet" extension
     * @param definition table definition
     * @param readInstructions instructions for customizations while reading
     * @return table
     */
    public static Table readTable(
            @NotNull final File sourceFile,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions readInstructions) {
        return readTableFromSingleParquetFile(sourceFile, readInstructions, definition);
    }

    /**
     * Write a table to a file.
     *
     * @param sourceTable source table
     * @param destPath destination file path; the file name should end in ".parquet" extension
     *                 If the path includes non-existing directories they are created
     *                 If there is an error any intermediate directories previously created are removed;
     *                 note this makes this method unsafe for concurrent use
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final String destPath) {
        writeTable(sourceTable, sourceTable.getDefinition(), ParquetInstructions.EMPTY, new File(destPath));
    }

    /**
     * Write a table to a file.
     *
     * @param sourceTable source table
     * @param destFile destination file; the file name should end in ".parquet" extension
     *             If the path includes non-existing directories they are created
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final File destFile) {
        writeTable(sourceTable, sourceTable.getDefinition(), ParquetInstructions.EMPTY, destFile);
    }

    /**
     * Write a table to a file.
     * @param sourceTable source table
     * @param definition table definition to use (instead of the one implied by the table itself)
     * @param destFile destination file; its path must end in ".parquet".  Any non existing directories in the path are created
     *                 If there is an error any intermediate directories previously created are removed;
     *                 note this makes this method unsafe for concurrent use
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final TableDefinition definition,
            @NotNull final File destFile) {
        writeTable(sourceTable, definition, ParquetInstructions.EMPTY, destFile);
    }

    /**
     * Write a table to a file.
     * @param sourceTable source table
     * @param writeInstructions instructions for customizations while writing
     * @param destFile destination file; its path must end in ".parquet".  Any non existing directories in the path are created
     *                 If there is an error any intermediate directories previously created are removed;
     *                 note this makes this method unsafe for concurrent use
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final File destFile) {
        writeTable(sourceTable, sourceTable.getDefinition(), writeInstructions, destFile);
    }

    /**
     * Write a table to a file.
     * @param sourceTable source table
     * @param definition table definition to use (instead of the one implied by the table itself)
     * @param writeInstructions instructions for customizations while writing
     * @param destPath destination path; it must end in ".parquet".  Any non existing directories in the path are created
     *                     If there is an error any intermediate directories previously created are removed;
     *                     note this makes this method unsafe for concurrent use
     */
    public static void writeTable(@NotNull final Table sourceTable,
                                  @NotNull final TableDefinition definition,
                                  @NotNull final ParquetInstructions writeInstructions,
                                  @NotNull final String destPath) {
        writeTable(sourceTable, definition, writeInstructions, new File(destPath));
    }

    /**
     * Write a table to a file.
     * @param sourceTable source table
     * @param definition table definition to use (instead of the one implied by the table itself)
     * @param writeInstructions instructions for customizations while writing
     * @param destFile destination file; its path must end in ".parquet".  Any non existing directories in the path are created
     *                 If there is an error any intermediate directories previously created are removed;
     *                 note this makes this method unsafe for concurrent use
     */
    public static void writeTable(@NotNull final Table sourceTable,
                                  @NotNull final TableDefinition definition,
                                  @NotNull final ParquetInstructions writeInstructions,
                                  @NotNull final File destFile) {
        final File firstCreated = prepareDestinationFileLocation(destFile);
        try {
            writeParquetTableImpl(
                    sourceTable, definition, writeInstructions, destFile, definition.getGroupingColumnNamesArray());
        } catch (Exception e) {
            if (firstCreated != null) {
                FileUtils.deleteRecursivelyOnNFS(firstCreated);
            } else {
                destFile.delete();
            }
            throw e;
        }
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
            throw new UncheckedDeephavenException("Destination " + destination + " does not end in " + PARQUET_FILE_EXTENSION + " extension");
        }
        if (destination.exists()) {
            if (destination.isDirectory()) {
                throw new UncheckedDeephavenException("Destination " + destination + " exists and is a directory");
            }
            if (!destination.canWrite()) {
                throw new UncheckedDeephavenException("Destination " + destination + " exists but is not writable");
            }
            return null;
        }
        final File firstParent = destination.getParentFile();
        if (firstParent.isDirectory()) {
            if (firstParent.canWrite()) {
                return null;
            }
            throw new UncheckedDeephavenException("Destination " + destination + " has non writable parent directory");
        }
        File firstCreated = firstParent;
        File parent;
        for (parent = destination.getParentFile(); parent != null && !parent.exists(); parent = parent.getParentFile()) {
            firstCreated = parent;
        }
        if (parent == null) {
            throw new IllegalArgumentException("Can't find any existing parent directory for destination path: " + destination);
        }
        if (!parent.isDirectory()) {
            throw new IllegalArgumentException("Existing parent file " + parent + " of " + destination + " is not a directory");
        }
        if (!firstParent.mkdirs()) {
            throw new UncheckedDeephavenException("Couldn't (re)create destination directory " + firstParent);
        }
        return firstCreated;
    }

    /**
     * Writes tables to disk in parquet format to a supplied set of destinations.  If you specify grouping columns, there
     * must already be grouping information for those columns in the sources.  This can be accomplished with
     * {@code .by(<grouping columns>).ungroup()} or {@code .sort(<grouping column>)}.
     *
     * @param sources            The tables to write
     * @param tableDefinition    The common schema for all the tables to write
     * @param writeInstructions  Write instructions for customizations while writing
     * @param destinations    The destinations paths.    Any non existing directories in the paths provided are created.
     *                        If there is an error any intermediate directories previously created are removed;
     *                        note this makes this method unsafe for concurrent use
     * @param groupingColumns List of columns the tables are grouped by (the write operation will store the grouping info)
     */
    public static void writeParquetTables(@NotNull final Table[] sources,
                                          @NotNull final TableDefinition tableDefinition,
                                          @NotNull final ParquetInstructions writeInstructions,
                                          @NotNull final File[] destinations,
                                          @NotNull final String[] groupingColumns) {
        Require.eq(sources.length, "sources.length", destinations.length, "destinations.length");
        final File[] absoluteDestinations =
                Arrays.stream(destinations)
                        .map(File::getAbsoluteFile)
                        .toArray(File[]::new);
        final File[] firstCreatedDirs =
                Arrays.stream(absoluteDestinations)
                        .map(ParquetTools::prepareDestinationFileLocation)
                        .toArray(File[]::new);
        for (int i = 0; i < sources.length; i++) {
            final Table source = sources[i];
            try {
                writeParquetTableImpl(source, tableDefinition, writeInstructions, destinations[i], groupingColumns);
            } catch (RuntimeException e) {
                for (final File destination : destinations) {
                    destination.delete();
                }
                for (final File firstCreatedDir : firstCreatedDirs) {
                    if (firstCreatedDir == null) {
                        continue;
                    }
                    log.error().append("Error in table writing, cleaning up potentially incomplete table destination path starting from ")
                            .append(firstCreatedDir.getAbsolutePath())
                            .append(e);
                    FileUtils.deleteRecursivelyOnNFS(firstCreatedDir);
                }
                throw e;
            }
        }
    }

    /**
     * Write out tables to disk.
     *
     * @param sources source tables
     * @param tableDefinition table definition
     * @param destinations destinations
     */
    public static void writeTables(@NotNull final Table[] sources,
                                   @NotNull final TableDefinition tableDefinition,
                                   @NotNull final File[] destinations) {
        writeParquetTables(sources, tableDefinition, ParquetInstructions.EMPTY, destinations,
                tableDefinition.getGroupingColumnNamesArray());
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

    private static Table readTableFromSingleParquetFile(
            @NotNull final File sourceFile,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final TableDefinition tableDefinition) {
        final TableLocationProvider locationProvider = new PollingTableLocationProvider(
                StandaloneTableKey.getInstance(),
                new ParquetTableLocationScanner(new SingleFile(sourceFile), readInstructions),
                null);
        return new SimpleSourceTable(tableDefinition.getWritable(), "Read single parquet file from " + sourceFile,
                RegionedTableComponentFactoryImpl.INSTANCE, locationProvider, null);
    }

    /**
     * {@link ParquetTableLocationScanner.LocationKeyFinder Parquet location finder} that will discover a single file.
     */
    public static final class SingleFile implements ParquetTableLocationScanner.LocationKeyFinder {

        private final File parquetFile;

        /**
         * @param parquetFile The single parquet file to find
         */
        public SingleFile(@NotNull final File parquetFile) {
            this.parquetFile = parquetFile;
        }

        @Override
        public void findKeys(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
            doFilesystemAction(() -> {
                locationKeyObserver.accept(new ParquetTableLocationKey(parquetFile, null));
            });
        }
    }

    /**
     * {@link ParquetTableLocationScanner.LocationKeyFinder Parquet location finder} that will discover multiple files
     * in a single directory.
     */
    public static final class FlatLayout implements ParquetTableLocationScanner.LocationKeyFinder {

        private final File tableRootDirectory;

        /**
         * @param tableRootDirectory The directory to search for .parquet files.
         */
        public FlatLayout(@NotNull final File tableRootDirectory) {
            this.tableRootDirectory = tableRootDirectory;
        }

        @Override
        public void findKeys(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
            doFilesystemAction(() -> {
                try (final DirectoryStream<Path> parquetFileStream = Files.newDirectoryStream(tableRootDirectory.toPath(), "*" + PARQUET_FILE_EXTENSION)) {
                    for (final Path parquetFilePath : parquetFileStream) {
                        locationKeyObserver.accept(new ParquetTableLocationKey(parquetFilePath.toFile(), null));
                    }
                } catch (final IOException e) {
                    throw new TableDataException("FlatLayout: Error finding parquet locations under " + tableRootDirectory, e);
                }
            });
        }
    }

    /**
     * {@link ParquetTableLocationScanner.LocationKeyFinder Parquet location finder} that will traverse a directory
     * hierarchy and infer partitions from key-value paris in the directory names, e.g.
     * <pre>tableRootDirectory/Country=France/City=Paris/parisData.parquet</pre>.
     * TODO (https://github.com/deephaven/deephaven-core/issues/858): Implement this
     */
    public static final class HiveStylePartitionLayout implements ParquetTableLocationScanner.LocationKeyFinder {

        private final File tableRootDirectory;
        private boolean inferPartitioningColumnTypes;

        /**
         * @param tableRootDirectory           The directory to traverse from
         * @param inferPartitioningColumnTypes Whether to infer partitioning column types.
         *                                     {@code false} means always use {@code String}.
         */
        public HiveStylePartitionLayout(@NotNull final File tableRootDirectory,
                                        final boolean inferPartitioningColumnTypes) {
            this.tableRootDirectory = tableRootDirectory;
            this.inferPartitioningColumnTypes = inferPartitioningColumnTypes;
        }

        @Override
        public void findKeys(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
            throw new UnsupportedOperationException("This is just a placeholder, see: https://github.com/deephaven/deephaven-core/issues/858");
        }
    }

    /**
     * {@link ParquetTableLocationScanner.LocationKeyFinder Parquet location finder} that will traverse a directory
     * hierarchy laid out in Deephaven's "nested-partitioned" format, e.g.
     * <pre>tableRootDirectory/internalPartitionValue/columnPartitionValue/tableName/table.parquet</pre>, producing
     * {@link ParquetTableLocationKey}'s with two partitions, for keys {@value INTERNAL_PARTITION_KEY} and the specified
     * {@code columnPartitionKey}.
     */
    public static final class DeephavenStylePartitionLayout implements ParquetTableLocationScanner.LocationKeyFinder {

        @VisibleForTesting
        public static final String PARQUET_FILE_NAME = "table.parquet";
        public static final String INTERNAL_PARTITION_KEY = "__INTERNAL_PARTITION__";

        private final File tableRootDirectory;
        private final String tableName;
        private final String columnPartitionKey;
        private final Predicate<String> internalPartitionValueFilter;

        /**
         * @param tableRootDirectory           The directory to traverse from
         * @param tableName                    The table name
         * @param columnPartitionKey           The partitioning column name
         * @param internalPartitionValueFilter Filter to control which internal partitions are included, {@code null} for all
         */
        public DeephavenStylePartitionLayout(@NotNull final File tableRootDirectory,
                                             @NotNull final String tableName,
                                             @NotNull final String columnPartitionKey,
                                             @Nullable final Predicate<String> internalPartitionValueFilter) {
            this.tableRootDirectory = tableRootDirectory;
            this.tableName = tableName;
            this.columnPartitionKey = columnPartitionKey;
            this.internalPartitionValueFilter = internalPartitionValueFilter;
        }

        @Override
        public void findKeys(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
            final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
            doFilesystemAction(() -> {
                try (final DirectoryStream<Path> internalPartitionStream = Files.newDirectoryStream(tableRootDirectory.toPath(), Files::isDirectory)) {
                    for (final Path internalPartition : internalPartitionStream) {
                        final String internalPartitionValue = internalPartition.getFileName().toString();
                        if (internalPartitionValueFilter != null && !internalPartitionValueFilter.test(internalPartitionValue)) {
                            continue;
                        }
                        boolean needToUpdateInternalPartitionValue = true;
                        try (final DirectoryStream<Path> columnPartitionStream = Files.newDirectoryStream(internalPartition, Files::isDirectory)) {
                            for (final Path columnPartition : columnPartitionStream) {
                                partitions.put(columnPartitionKey, columnPartition.getFileName().toFile());
                                if (needToUpdateInternalPartitionValue) {
                                    // Partition order dictates comparison priority, so we need to insert the internal partition after the column partition.
                                    partitions.put(INTERNAL_PARTITION_KEY, internalPartitionValue);
                                    needToUpdateInternalPartitionValue = false;
                                }
                                locationKeyObserver.accept(new ParquetTableLocationKey(columnPartition.resolve(tableName).resolve(PARQUET_FILE_NAME).toFile(), partitions));
                            }
                        }
                    }
                } catch (final NoSuchFileException | FileNotFoundException ignored) {
                    // If we found nothing at all, then there's nothing to be done at this level.
                } catch (final IOException e) {
                    throw new TableDataException("DeephavenStylePartitionLayout-" + tableName + ": Error finding locations under " + tableRootDirectory, e);
                }
            });
        }
    }

    /**
     * Wrap a filesystem operation (or series of them) as a privileged action.
     *
     * @param operation The operation to wrap
     */
    private static void doFilesystemAction(@NotNull final Runnable operation) {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                operation.run();
                return null;
            });
        } catch (final PrivilegedActionException pae) {
            if (pae.getException() instanceof TableDataException) {
                throw (TableDataException) pae.getException();
            } else {
                throw new UncheckedDeephavenException(pae.getException());
            }
        }
    }

    /**
     * Reads in a table from a file, using the provided table definition.
     *
     * @param sourceFile table location; the file should exist and end in ".parquet" extension
     * @param definition table definition
     * @param readInstructions instructions for customizations while reading
     * @return table
     */
    public static Table readMultiFileTable(
            @NotNull final File rootDirectory,
            @Nullable final Predicate<File> filter,
            @NotNull final Function<File[], TableDefinition> definitionProvider,
            @NotNull final ParquetInstructions readInstructions) {

        // Support "hive style" and "ordered" partitioning. Type inference?
        return readTableFromSingleParquetFile(sourceFile, readInstructions, definition);
    }

    private static final SimpleTypeMap<Class<?>> DB_ARRAY_TYPE_MAP = SimpleTypeMap.create(
            null, DbCharArray.class, DbByteArray.class, DbShortArray.class, DbIntArray.class, DbLongArray.class,
            DbFloatArray.class, DbDoubleArray.class, DbArray.class);

    private static Class<?> loadClass(final String colName, final String desc, final String className) {
        try {
            return ClassUtil.lookupClass(className);
        } catch (ClassNotFoundException e) {
            throw new UncheckedDeephavenException(
                    "Column " + colName + " with " + desc + "=" + className + " that can't be found in classloader");
        }
    }

    private static ParquetSchemaReader.ColumnDefinitionConsumer
    makeSchemaReaderConsumer(final ArrayList<ColumnDefinition> colsOut) {
        return (final ParquetSchemaReader.ParquetMessageDefinition parquetColDef) -> {
            Class<?> baseType;
            if (parquetColDef.baseType != null && parquetColDef.baseType == boolean.class) {
                baseType = Boolean.class;
            } else {
                baseType = parquetColDef.baseType;
            }
            ColumnDefinition<?> colDef;
            if (parquetColDef.codecType != null && !parquetColDef.codecType.isEmpty()) {
                final Class<?> componentType =
                (parquetColDef.codecComponentType != null && !parquetColDef.codecComponentType.isEmpty())
                        ? loadClass(parquetColDef.name, "codecComponentType", parquetColDef.codecComponentType)
                        : null
                        ;
                final Class<?> dataType = loadClass(parquetColDef.name, "codecType", parquetColDef.codecType);
                colDef = ColumnDefinition.fromGenericType(parquetColDef.name, dataType, componentType);
            } else if (parquetColDef.dhSpecialType != null) {
                if (parquetColDef.dhSpecialType.equals(ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                    colDef = ColumnDefinition.fromGenericType(parquetColDef.name, StringSet.class, null);
                } else if (parquetColDef.dhSpecialType.equals(ParquetTableWriter.DBARRAY_SPECIAL_TYPE)) {
                    final Class<?> dbArrayType = DB_ARRAY_TYPE_MAP.get(baseType);
                    if (dbArrayType != null) {
                        colDef = ColumnDefinition.fromGenericType(parquetColDef.name, dbArrayType, baseType);
                    } else {
                        colDef = ColumnDefinition.fromGenericType(parquetColDef.name, DbArray.class, baseType);
                    }
                } else {
                    throw new UncheckedDeephavenException("Unhandled dbSpecialType=" + parquetColDef.dhSpecialType);
                }
            } else {
                if (parquetColDef.isArray) {
                    if (baseType == byte.class && parquetColDef.noLogicalType) {
                        colDef = ColumnDefinition.fromGenericType(parquetColDef.name, byte[].class, byte.class);
                    } else {
                        // TODO: ParquetInstruction.loadAsDbArray
                        final Class<?> componentType = baseType;
                        // On Java 12, replace by:  dataType = componentType.arrayType();
                        final Class<?> dataType = java.lang.reflect.Array.newInstance(componentType, 0).getClass();
                        colDef = ColumnDefinition.fromGenericType(parquetColDef.name, dataType, componentType);
                    }
                } else {
                    colDef = ColumnDefinition.fromGenericType(parquetColDef.name, baseType, null);
                }
            }
            if (colDef.getDataType() == String.class && parquetColDef.dictionaryUsedOnEveryDataPage) {
                colDef = colDef.withSymbolTable();
            }
            if (parquetColDef.isGrouping) {
                colDef = colDef.withGrouping();
            }
            colsOut.add(colDef);
        };
    }

    private static Table readParquetSchemaAndTable(
            @NotNull final File source, @NotNull ParquetInstructions readInstructions) {
        return readParquetSchemaAndTable(source, readInstructions, null);
    }

    @VisibleForTesting
    public static Table readParquetSchemaAndTable(
            @NotNull final File source, @NotNull ParquetInstructions readInstructions, MutableObject<ParquetInstructions> instructionsOut) {
        // noinspection rawtypes
        final ArrayList<ColumnDefinition> cols = new ArrayList<>();
        final ParquetSchemaReader.ColumnDefinitionConsumer colConsumer = makeSchemaReaderConsumer(cols);

        try {
            final String path = source.getPath();
            readInstructions = ParquetSchemaReader.readParquetSchema(
                    path,
                    readInstructions,
                    colConsumer,
                    (final String colName, final Set<String> takenNames) ->
                            NameValidator.legalizeColumnName(
                                    colName, s -> s.replace(" ", "_"), takenNames));
            if (instructionsOut != null) {
                instructionsOut.setValue(readInstructions);
            }
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Error trying to load table definition from parquet file", e);
        }
        final TableDefinition def = new TableDefinition(cols);
        return readTableFromSingleParquetFile(source, readInstructions, def);
    }

    private static void writeParquetTableImpl(
            final Table sourceTable,
            final TableDefinition definition,
            final ParquetInstructions writeInstructions,
            final File destFile,
            final String[] groupingColumns) {
        final String path = destFile.getPath();
        try {
            if (groupingColumns.length > 0) {
                ParquetTableWriter.write(
                        sourceTable, definition, writeInstructions, path, Collections.emptyMap(),
                        ParquetTableWriter.defaultGroupingFileName(path), groupingColumns);
            } else {
                ParquetTableWriter.write(
                        sourceTable, definition, writeInstructions, path, Collections.emptyMap());
            }
        }
        catch (Exception e) {
            throw new UncheckedDeephavenException("Error writing table to " + destFile, e);
        }
    }

    public static final ParquetInstructions LZ4 = ParquetInstructions.builder().setCompressionCodecName("LZ4").build();
    public static final ParquetInstructions LZO = ParquetInstructions.builder().setCompressionCodecName("LZO").build();
    public static final ParquetInstructions GZIP = ParquetInstructions.builder().setCompressionCodecName("GZIP").build();
    public static final ParquetInstructions ZSTD = ParquetInstructions.builder().setCompressionCodecName("ZSTD").build();
    public static final ParquetInstructions LEGACY = ParquetInstructions.builder().setIsLegacyParquet(true).build();

    public static void setDefaultCompressionCodecName(final String compressionCodecName) {
        ParquetInstructions.setDefaultCompressionCodecName(compressionCodecName);
    }
}
