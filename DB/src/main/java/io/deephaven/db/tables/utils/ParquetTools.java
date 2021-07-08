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
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.parquet.ParquetTableWriter.PARQUET_FILE_EXTENSION;

import java.io.File;
import java.util.*;

/**
 * Tools for managing and manipulating tables on disk in parquet format.
 *
 */
@SuppressWarnings("WeakerAccess")
public class ParquetTools {

    private ParquetTools() {
    }

    private static final Logger log = LoggerFactory.getLogger(ParquetTools.class);

    /**
     * Reads in a table from disk.
     *
     * @param sourceFilePath table location; the file should exist and end in ".parquet" extension.
     * @return table
     */
    public static Table readTable(@NotNull final String sourceFilePath) {
        return readParquetSchemaAndTable(new File(sourceFilePath), ParquetInstructions.EMPTY);
    }

    /**
     * Reads in a table from disk.
     *
     * @param sourceFilePath table location; the file should exist and end in ".parquet" extension.
     * @param readInstructions instructions for customizations while reading
     * @return table
     */
    public static Table readTable(
            @NotNull final String sourceFilePath,
            @NotNull final ParquetInstructions readInstructions) {
        return readParquetSchemaAndTable(new File(sourceFilePath), readInstructions);
    }

    /**
     * Reads in a table from disk.
     *
     * @param sourceFile table location; the file should exist and end in ".parquet" extension.
     * @return table
     */
    public static Table readTable(@NotNull final File sourceFile) {
        return readParquetSchemaAndTable(sourceFile, ParquetInstructions.EMPTY);
    }

    /**
     * Reads in a table from disk.
     *
     * @param sourceFile table location; the file should exist and end in ".parquet" extension.
     * @param readInstructions instructions for customizations while reading
     * @return table
     */
    public static Table readTable(
            @NotNull final File sourceFile,
            @NotNull final ParquetInstructions readInstructions) {
        return readParquetSchemaAndTable(sourceFile, readInstructions);
    }

    /**
     * Reads in a table from disk, using the provided table definition.
     *
     * @param sourceFilePath table location; the file should exist and end in ".parquet" extension.
     * @param def table definition
     * @return table
     */
    public static Table readTable(
            @NotNull final String sourceFilePath,
            final TableDefinition def) {
        return readTableFromSingleParquetFile(new File(sourceFilePath), ParquetInstructions.EMPTY, def);
    }

    /**
     * Reads in a table from disk, using the provided table definition.
     *
     * @param sourceFilePath table location; the file should exist and end in ".parquet" extension.
     * @param def table definition
     * @param readInstructions instructions for customizations while reading
     * @return table
     */
    public static Table readTable(
            @NotNull final String sourceFilePath,
            @NotNull final TableDefinition def,
            @NotNull final ParquetInstructions readInstructions) {
        return readTableFromSingleParquetFile(new File(sourceFilePath), readInstructions, def);
    }

    /**
     * Reads in a table from disk, using the provided table definition.
     *
     * @param sourceFile table location; the file should exist and end in ".parquet" extension.
     * @param def table definition
     * @return table
     */
    public static Table readTable(
            @NotNull final File sourceFile,
            @NotNull final TableDefinition def) {
        return readTableFromSingleParquetFile(sourceFile, ParquetInstructions.EMPTY, def);
    }

    /**
     * Reads in a table from disk, using the provided table definition.
     *
     * @param sourceFile table location; the file should exist and end in ".parquet" extension.
     * @param def table definition
     * @param readInstructions instructions for customizations while reading
     * @return table
     */
    public static Table readTable(
            @NotNull final File sourceFile,
            @NotNull final TableDefinition def,
            @NotNull final ParquetInstructions readInstructions) {
        return readTableFromSingleParquetFile(sourceFile, readInstructions, def);
    }

    /**
     * Write out a table to disk.
     *
     * @param sourceTable source table
     * @param destPath destination file path; the file name should end in ".parquet" extension.
     *                 If the path includes non-existing directories they are created.
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final String destPath) {
        writeTable(sourceTable, sourceTable.getDefinition(), ParquetInstructions.EMPTY, new File(destPath));
    }

    /**
     * Write out a table to disk.
     *
     * @param sourceTable source table
     * @param dest destination file; the file name should end in ".parquet" extension.
     *             If the path includes non-existing directories they are created
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final File dest) {
        writeTable(sourceTable, sourceTable.getDefinition(), ParquetInstructions.EMPTY, dest);
    }

    /**
     * Write out a table to disk.
     * @param sourceTable source table
     * @param definition table definition.  Will be written to destination as given
     * @param destFile destination file; its path must end in ".parquet".  Any non existing directories in the path are created
     */
    public static void writeTable(
            @NotNull final Table sourceTable,
            @NotNull final TableDefinition definition,
            @NotNull final File destFile) {
        writeTable(sourceTable, definition, ParquetInstructions.EMPTY, destFile);
    }

    /**
     * Write out a table to disk.
     * @param sourceTable source table
     * @param definition table definition.  Will be written to destination as given
     * @param writeInstructions instructions for customizations while writing
     * @param destFilePath destination path; it must end in ".parquet".  Any non existing directories in the path are created
     */
    public static void writeTable(@NotNull final Table sourceTable,
                                  @NotNull final TableDefinition definition,
                                  @NotNull final ParquetInstructions writeInstructions,
                                  @NotNull final String destFilePath) {
        writeTable(sourceTable, definition, writeInstructions, new File(destFilePath));
    }

    /**
     * Write out a table to disk.
     * @param sourceTable source table
     * @param definition table definition.  Will be written to destination as given
     * @param writeInstructions instructions for customizations while writing
     * @param destFile destination file; its path must end in ".parquet".  Any non existing directories in the path are created
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
            }
            throw e;
        }
    }

    /**
     * Make the parent directory of destination if it does not exist, and any missing parents.
     *
     * @param destination The destination file
     * @return The first created directory, or null, if no directories were made.
     */
    private static File prepareDestinationFileLocation(@NotNull File destination) {
        destination = destination.getAbsoluteFile();
        if (!destination.getPath().endsWith(PARQUET_FILE_EXTENSION)) {
            throw new UncheckedDeephavenException("Destination " + destination + " does not end in " + PARQUET_FILE_EXTENSION + " extension.");
        }
        if (destination.exists()) {
            if (destination.isDirectory()) {
                throw new UncheckedDeephavenException("Destination " + destination + " exists and is a directory.");
            }
            if (!destination.canWrite()) {
                throw new UncheckedDeephavenException("Destination " + destination + " exists but is not writable.");
            }
            return null;
        }
        final File firstParent = destination.getParentFile();
        if (firstParent.exists()) {
            if (firstParent.canWrite()) {
                return null;
            }
            throw new UncheckedDeephavenException("Destination " + destination + " has non writable parent directory.");
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
     * Writes tables to disk in parquet format under a given destinations.  If you specify grouping columns, there
     * must already be grouping information for those columns in the sources.  This can be accomplished with
     * {@code .by(<grouping columns>).ungroup()} or {@code .sort(<grouping column>)}.
     *
     * @param sources            The tables to write
     * @param tableDefinition    The common schema for all the tables to write
     * @param writeInstructions  Write instructions for customizations while writing
     *
     * @param destinations    The destinations path
     * @param groupingColumns List of columns the tables are grouped by (the write operation will store the grouping info)
     */
    public static void writeParquetTables(@NotNull final Table[] sources,
                                          @NotNull final TableDefinition tableDefinition,
                                          @NotNull final ParquetInstructions writeInstructions,
                                          @NotNull final File[] destinations, String[] groupingColumns) {
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
                for (final File firstCreatedDir : firstCreatedDirs) {
                    if (firstCreatedDir == null) {
                        continue;
                    }
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
        writeParquetTables(sources, tableDefinition, ParquetInstructions.EMPTY, destinations,
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

    private static Table readTableFromSingleParquetFile(
            @NotNull final File sourceFile,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final TableDefinition tableDefinition) {
        final TableLocationProvider locationProvider = new ReadOnlyLocalTableLocationProviderByParquetFile(
                StandaloneTableKey.getInstance(),
                sourceFile,
                false,
                TableDataRefreshService.getSharedRefreshService(),
                readInstructions);
        return new SimpleSourceTable(tableDefinition.getWritable(), "Read single parquet file from " + sourceFile,
                RegionedTableComponentFactoryImpl.INSTANCE, locationProvider, null);
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
        if (componentTypeFromParquet.equals(Boolean.class)) {
            return DbArray.class;
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

    private static ParquetReaderUtil.ColumnDefinitionConsumer makeSchemaReaderConsumer(final ArrayList<ColumnDefinition> cols) {
        return (final String name, final Class<?> typeFromParquet, final String dbSpecialType, final boolean isLegacyType, final boolean isArray,
                final boolean isGrouping, final String codecType, final String codecComponentType) -> {
            Class<?> baseType;
            if (typeFromParquet != null && typeFromParquet.equals(boolean.class)) {
                baseType = Boolean.class;
            } else {
                baseType = typeFromParquet;
            }
            final ColumnDefinition<?> colDef;
            if (codecType != null && !codecType.isEmpty()) {
                final Class<?> componentType =
                (codecComponentType != null && !codecComponentType.isEmpty())
                        ? loadClass(name, "codecComponentType", codecComponentType)
                        : null
                        ;
                final Class<?> dataType = loadClass(name, "codecType", codecType);
                colDef = ColumnDefinition.fromGenericType(name, dataType, componentType);
            } else if (dbSpecialType != null) {
                if (dbSpecialType.equals(ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                    colDef = ColumnDefinition.fromGenericType(name, StringSet.class, null);
                } else if (dbSpecialType.equals(ParquetTableWriter.DBARRAY_SPECIAL_TYPE)) {
                    final Class<?> dbArrayType = dbArrayType(baseType);
                    if (dbArrayType != null) {
                        colDef = ColumnDefinition.fromGenericType(name, dbArrayType, baseType);
                    } else {
                        colDef = ColumnDefinition.fromGenericType(name, DbArray.class, baseType);
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
        final ParquetReaderUtil.ColumnDefinitionConsumer colConsumer = makeSchemaReaderConsumer(cols);

        try {
            final String path = source.getPath();
            readInstructions = ParquetReaderUtil.readParquetSchema(
                    path,
                    readInstructions,
                    colConsumer,
                    (final String colName, final Set<String> takenNames) ->
                            DBNameValidator.legalizeColumnName(
                                    colName, s -> s.replace(" ", "_"), takenNames));
            if (instructionsOut != null) {
                instructionsOut.setValue(readInstructions);
            }
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Error trying to load table definition from parquet file: " + e, e);
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
                        sourceTable, path, Collections.emptyMap(), writeInstructions, definition,
                        ParquetTableWriter.defaultGroupingFileName(path), groupingColumns);
            } else {
                ParquetTableWriter.write(
                        sourceTable, definition, path, Collections.emptyMap(), writeInstructions);
            }
        }
        catch (Exception e) {
            throw new UncheckedDeephavenException("Error writing table to " + destFile + ": " + e, e);
        }
    }
}
