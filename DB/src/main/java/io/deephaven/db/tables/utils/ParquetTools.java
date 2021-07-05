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
import io.deephaven.db.v2.locations.local.ReadOnlyLocalTableLocationProviderByParquetFile;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.db.v2.parquet.ParquetReaderUtil;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.SimpleSourceTable;
import io.deephaven.db.v2.locations.StandaloneTableKey;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.db.v2.locations.local.ReadOnlyLocalTableLocationProvider;
import io.deephaven.db.v2.locations.local.StandaloneLocalTableLocationScanner;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.codec.ObjectDecoder;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.parquet.ParquetTableWriter.PARQUET_FILE_EXTENSION;

import java.io.File;
import java.util.*;

/**
 * Tools for managing and manipulating tables on disk.
 *
 * Most users will need {@link TableTools} and not {@link ParquetTools}.
 */
@SuppressWarnings("WeakerAccess")
public class ParquetTools {

    private ParquetTools() {
    }

    private static final Logger log = LoggerFactory.getLogger(ParquetTools.class);

    ///////////  Utilities For Table I/O /////////////////

    private static Table getTable(final String description, final TableDefinition sourceDef, final TableLocationProvider locationProvider) {
        TableDefinition tableDefinition = sourceDef.getWritable();
        return new SimpleSourceTable(tableDefinition, description, RegionedTableComponentFactoryImpl.INSTANCE, locationProvider, null);
    }

    private static volatile CompressionCodecName defaultPerquetCompressionCodec = CompressionCodecName.SNAPPY;

    /**
     * Sets the default parquet compression codec for writing parquet.
     * @param codecName the codec name.
     */
    public static void setDefaultParquetCompressionCodec(final String codecName) {
        defaultPerquetCompressionCodec = CompressionCodecName.valueOf(codecName);
    }

    /**
     * Reads in a table from disk.
     *
     * @param location table location; if it ends in ".parquet" is assumed to be a single file location, otherwise is a directory.
     * @param tableDefinition table definition
     * @return table
     */
    public static Table readTable(@NotNull final File location, @NotNull TableDefinition tableDefinition) {
        return readTable(location, null, tableDefinition);
    }

    public static Table readTable(
            @NotNull final File location, final ParquetInstructions readInstructions, @NotNull TableDefinition tableDefinition) {
        final String path = location.getPath();
        if (path.endsWith(PARQUET_FILE_EXTENSION)) {
            return readTableFromSingleParquetFile(location, readInstructions, tableDefinition);
        }
        final TableLocationProvider locationProvider = new ReadOnlyLocalTableLocationProvider(
                StandaloneTableKey.getInstance(),
                new StandaloneLocalTableLocationScanner(location),
                false,
                TableDataRefreshService.getSharedRefreshService(),
                readInstructions != null ? readInstructions : ParquetInstructions.EMPTY);
        return getTable("Stand-alone V2 table from " + path, tableDefinition, locationProvider);
    }

    private static Table readTableFromSingleParquetFile(
            @NotNull final File sourceFile, final ParquetInstructions readInstructions, @NotNull final TableDefinition tableDefinition) {
        final TableLocationProvider locationProvider = new ReadOnlyLocalTableLocationProviderByParquetFile(
                StandaloneTableKey.getInstance(),
                sourceFile,
                false,
                TableDataRefreshService.getSharedRefreshService(),
                readInstructions != null ? readInstructions : ParquetInstructions.EMPTY);
        return getTable("Read single parquet file from " + sourceFile, tableDefinition, locationProvider);
    }

    /**
     * Reads in a table from disk.
     *
     * @param sourceFilePath table location; if it ends in ".parquet" is assumed to be a single file location, otherwise is a directory.
     * @return table
     */
    public static Table readTable(@NotNull final String sourceFilePath) {
        return readParquetTable(new File(sourceFilePath), !sourceFilePath.endsWith(PARQUET_FILE_EXTENSION));
    }

    /**
     * Reads in a table from disk.
     *
     * @param sourceFilePath table location; if its path ends in ".parquet" is assumed to be a single file location, otherwise is a directory.
     * @return table
     */
    public static Table readTable(@NotNull final File sourceFilePath) {
        return readParquetTable(sourceFilePath, !sourceFilePath.getPath().endsWith(PARQUET_FILE_EXTENSION));
    }

    private static Class<?> dbArrayType(final Class<?> componentTypeFromParquet) {
        if (componentTypeFromParquet == null) {
            return null;
        }
        if (componentTypeFromParquet.equals(int.class)) {
            return DbIntArray.class;
        }
        if (componentTypeFromParquet.equals(long.class)) {
            return DbLongArray.class;
        }
        if (componentTypeFromParquet.equals(byte.class)) {
            return DbByteArray.class;
        }
        if (componentTypeFromParquet.equals(double.class)) {
            return DbDoubleArray.class;
        }
        if (componentTypeFromParquet.equals(float.class)) {
            return DbFloatArray.class;
        }
        if (componentTypeFromParquet.equals(char.class)) {
            return DbCharArray.class;
        }
        if (componentTypeFromParquet.equals(short.class)) {
            return DbShortArray.class;
        }
        if (componentTypeFromParquet.equals(boolean.class)) {
            return DbBooleanArray.class;
        }
        return null;
    }

    private static Class<?> loadClass(final String colName, final String desc, final String className) {
        try {
            return ClassUtil.lookupClass(className);
        } catch (ClassNotFoundException e) {
            throw new UncheckedDeephavenException(
                    "Column " + colName + " with " + desc + "=" + className + " that can't be found in classloader.");
        }
    }

    private static Table readParquetTable(@NotNull final File source, final boolean isDirectory) {
        // noinspection rawtypes
        final ArrayList<ColumnDefinition> cols = new ArrayList<>();
        final ParquetReaderUtil.ColumnDefinitionConsumer colConsumer =
                (final String name, final Class<?> typeFromParquet, final String dbSpecialType, final boolean isLegacyType, final boolean isArray,
                 final boolean isGrouping, final String codecName, final String codecArgs, final String codecType, final String codecComponentType) -> {
                    Class<?> baseType;
                    if (typeFromParquet != null && typeFromParquet.equals(boolean.class)) {
                        baseType = Boolean.class;
                    } else {
                        baseType = typeFromParquet;
                    }
                    final ColumnDefinition<?> colDef;
                    if (codecName != null) {
                        Class<?> dataType = baseType;
                        Class<?> componentType = null;
                        if (codecType != null && !codecType.isEmpty()) {
                            if (codecComponentType != null && !codecComponentType.isEmpty()) {
                                componentType = loadClass(name, "codecComponentType", codecComponentType);
                            }
                            dataType = loadClass(name, "codecType", codecType);
                        }
                        final ObjectCodec<?> codec = CodecCache.DEFAULT.getCodec(codecName, codecArgs);
                        final int width = codec.expectedObjectWidth();
                        if (width != ObjectDecoder.VARIABLE_WIDTH_SENTINEL) {
                            colDef = ColumnDefinition.ofFixedWidthCodec(name, dataType, componentType, codecName, codecArgs, width);
                        } else {
                            colDef = ColumnDefinition.ofVariableWidthCodec(name, dataType, componentType, codecName, codecArgs);
                        }
                    } else if (dbSpecialType != null) {
                        if (dbSpecialType.equals(ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                            colDef = ColumnDefinition.fromGenericType(name, StringSet.class, null);
                        } else if (dbSpecialType.equals(ParquetTableWriter.DBARRAY_SPECIAL_TYPE)) {
                            final Class<?> dbArrayType = dbArrayType(baseType);
                            if (dbArrayType != null) {
                                colDef = ColumnDefinition.fromGenericType(name, dbArrayType, baseType);
                            } else {
                               throw new UncheckedDeephavenException("Unhandled dbArrayType=" +
                                       ((baseType == null) ? "null" : baseType.getSimpleName()));
                            }
                        } else {
                            throw new UncheckedDeephavenException("Unhandled dbSpecialType=" + dbSpecialType);
                        }
                    } else {
                        if (!StringSet.class.isAssignableFrom(baseType) && isArray) {
                            if (baseType.equals(byte.class) && isLegacyType) {
                                colDef = ColumnDefinition.fromGenericType(name, byte[].class, byte.class);
                            } else {
                                // TODO: ParquetInstruction.loadAsDbArray
                                final Class<?> componentType = baseType;
                                // On Java 12, replace by:  dataType = componentType.arrayType();
                                final Class<?> dataType = java.lang.reflect.Array.newInstance(componentType, 0).getClass();
                                colDef = ColumnDefinition.fromGenericType(name, dataType, componentType);
                            }
                        } else {
                            colDef = ColumnDefinition.fromGenericType(name, baseType, null);
                        }
                    }
                    cols.add(isGrouping ? colDef.withGrouping() : colDef);
                };
        ParquetInstructions readInstructions = ParquetInstructions.EMPTY;
        try {
            final String path = source.getPath() + ((!isDirectory) ? "" : File.separator + ParquetTableWriter.PARQUET_FILE_NAME);
            readInstructions = ParquetReaderUtil.readParquetSchema(
                    path,
                    readInstructions,
                    colConsumer,
                    (final String colName, final Set<String> takenNames) ->
                            DBNameValidator.legalizeColumnName(
                                    colName, s -> s.replace(" ", "_"), takenNames));
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Error trying to load table definition from parquet file: " + e, e);
        }
        final TableDefinition def = new TableDefinition(cols);
        return isDirectory
                ? ParquetTools.readTable(source, readInstructions, def)
                : readTableFromSingleParquetFile(source, readInstructions, def)
                ;
    }

    /**
     * Write out a table to disk.
     *
     * @param sourceTable source table
     * @param destPath destination file path; if it ends in ".parquet", it is assumed to be a file, otherwise a directory.
     */
    public static void writeTable(Table sourceTable, String destPath) {
        writeTable(sourceTable, sourceTable.getDefinition(), new File(destPath));
    }

    /**
     * Write out a table to disk.
     *
     * @param sourceTable source table
     * @param dest destination; if its path ends in ".parquet", it is assumed to be a single file location, otherwise a directory.
     */
    public static void writeTable(Table sourceTable, File dest) {
        writeTable(sourceTable, sourceTable.getDefinition(), dest);
    }

    /**
     * Write out a table to disk.
     *  @param sourceTable source table
     * @param definition table definition.  Will be written to disk as given.
     * @param destFile destination file; if its path ends in ".parquet", it is assumed to be a single file location path, otherwise a directory.
     */
    public static void writeTable(final Table sourceTable, final TableDefinition definition, final File destFile) {
        try {
            final String path = destFile.getPath();
            final String[] groupingColumns = definition.getGroupingColumnNamesArray();
            if (path.endsWith(PARQUET_FILE_EXTENSION)) {
                if (groupingColumns.length > 0) {
                    ParquetTableWriter.write(
                            sourceTable, path, Collections.emptyMap(), defaultPerquetCompressionCodec, sourceTable.getDefinition().getWritable(),
                            ParquetTableWriter.defaultGroupingFileName(path), groupingColumns);
                } else {
                    ParquetTableWriter.write(
                            sourceTable, sourceTable.getDefinition(), path, Collections.emptyMap(), defaultPerquetCompressionCodec);
                }
            } else {
                writeParquetTable(sourceTable, definition, defaultPerquetCompressionCodec, destFile, groupingColumns);
            }
        } catch (Exception e) {
            throw new UncheckedDeephavenException("Error writing table to " + destFile + ": " + e, e);
        }
    }

    /**
     * Delete the destination directory if it exists, then make the destination directory and any missing parent
     * directories, returning the first created for later rollback.
     *
     * @param destination The destination directory
     * @return The first created directory
     */
    static File prepareDestination(@NotNull File destination) {
        destination = destination.getAbsoluteFile();
        if (destination.exists()) {
            FileUtils.deleteRecursivelyOnNFS(destination);
        }
        File firstCreated = destination;
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
        if (!destination.mkdirs()) {
            throw new RuntimeException("Couldn't (re)create destination directory " + destination);
        }
        return firstCreated;
    }

    private static void writeParquetTableImpl(
            final Table source,
            final TableDefinition tableDefinition,
            final CompressionCodecName codecName,
            final File destinationDir,
            final String[] groupingColumns) {
        final String basePath = destinationDir.getPath();
        try {
            ParquetTableWriter.write(
                    source,
                    defaultParquetPath(basePath),
                    Collections.emptyMap(),
                    codecName,
                    tableDefinition.getWritable(),
                    ParquetTableWriter.defaultGroupingFileName(basePath),
                    groupingColumns);
        } catch (Exception e) {
            throw new RuntimeException("Error in table writing", e);
        }
    }

    private static String defaultParquetPath(final String dirPath) {
        return dirPath + File.separator + ParquetTableWriter.PARQUET_FILE_NAME;
    }

    /**
     * Writes a table to disk in parquet format under a given destination.  If you specify grouping columns, there
     * must already be grouping information for those columns in the source.  This can be accomplished with
     * {@code .by(<grouping columns>).ungroup()} or {@code .sort(<grouping column>)}.
     *
     * @param source          The table to write
     * @param tableDefinition The schema for the tables to write
     * @param codecName       Compression codec to use.
     *
     * @param destinationDir     The destination path
     * @param groupingColumns List of columns the tables are grouped by (the write operation will store the grouping info)
     */
    public static void writeParquetTable(
            @NotNull final Table source,
            @NotNull final TableDefinition tableDefinition,
            final CompressionCodecName codecName,
            @NotNull final File destinationDir,
            final String[] groupingColumns) {
        final File firstCreatedDir = prepareDestination(destinationDir.getAbsoluteFile());

        try {
            writeParquetTableImpl(source, tableDefinition, codecName, destinationDir, groupingColumns);
        } catch (RuntimeException e) {
            log.error("Error in table writing, cleaning up potentially incomplete table destination path starting from " +
                    firstCreatedDir.getAbsolutePath(), e);
            FileUtils.deleteRecursivelyOnNFS(firstCreatedDir);
            throw e;
        }
    }

    /**
     * Writes tables to disk in parquet format under a given destinations.  If you specify grouping columns, there
     * must already be grouping information for those columns in the sources.  This can be accomplished with
     * {@code .by(<grouping columns>).ungroup()} or {@code .sort(<grouping column>)}.
     *
     * @param sources         The tables to write
     * @param tableDefinition The common schema for all the tables to write
     * @param codecName       Compression codec to use.
     *
     * @param destinations    The destinations path
     * @param groupingColumns List of columns the tables are grouped by (the write operation will store the grouping info)
     */
    public static void writeParquetTables(@NotNull final Table[] sources,
                                          @NotNull final TableDefinition tableDefinition,
                                          final CompressionCodecName codecName,
                                          @NotNull final File[] destinations, String[] groupingColumns) {
        Require.eq(sources.length, "sources.length", destinations.length, "destinations.length");
        final File[] absoluteDestinations = Arrays.stream(destinations).map(File::getAbsoluteFile).toArray(File[]::new);
        final File[] firstCreatedDirs = Arrays.stream(absoluteDestinations).map(ParquetTools::prepareDestination).toArray(File[]::new);

        for (int i = 0; i < sources.length; i++) {
            final Table source = sources[i];
            try {
                writeParquetTableImpl(source, tableDefinition, codecName, destinations[i], groupingColumns);
            } catch (RuntimeException e) {
                for (final File firstCreatedDir : firstCreatedDirs) {
                    log.error("Error in table writing, cleaning up potentially incomplete table destination path starting from " +
                            firstCreatedDir.getAbsolutePath(), e);
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
        writeParquetTables(sources, tableDefinition, defaultPerquetCompressionCodec, destinations,
                tableDefinition.getGroupingColumnNamesArray());
    }

    /**
     * Deletes a table on disk.
     *
     * @param path path to delete
     */
    public static void deleteTable(File path) {
        FileUtils.deleteRecursivelyOnNFS(path);
    }
}
