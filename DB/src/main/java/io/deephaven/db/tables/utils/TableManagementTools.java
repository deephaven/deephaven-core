/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.*;

/**
 * Tools for managing and manipulating tables on disk.
 *
 * Most users will need {@link TableTools} and not {@link TableManagementTools}.
 *
 * @IncludeAll
 */
@SuppressWarnings("WeakerAccess")
public class TableManagementTools {

    public enum StorageFormat {
        Parquet
    }

    private TableManagementTools() {
    }

    private static final Logger log = LoggerFactory.getLogger(TableManagementTools.class);

    ///////////  Utilities For Table I/O /////////////////

    /**
     * Reads in a table from disk.
     *
     * @param path table location
     * @param tableDefinition table definition
     * @return table
     */
    public static Table readTable(@NotNull final File path, @NotNull TableDefinition tableDefinition) {
        tableDefinition = tableDefinition.getWritable();
        if (tableDefinition.getStorageType() == TableDefinition.STORAGETYPE_NESTEDPARTITIONEDONDISK) {
            tableDefinition = new TableDefinition(tableDefinition);
            tableDefinition.setStorageType(TableDefinition.STORAGETYPE_SPLAYEDONDISK);
        }
        final String description = "Stand-alone V2 table from " + path;
        final TableLocationProvider locationProvider = new ReadOnlyLocalTableLocationProvider(
                StandaloneTableKey.getInstance(),
                new StandaloneLocalTableLocationScanner(path),
                false,
                TableDataRefreshService.getSharedRefreshService());
        return new SimpleSourceTable(tableDefinition, description, RegionedTableComponentFactoryImpl.INSTANCE, locationProvider, null);
    }

    /**
     * Reads in a table from disk.
     *
     * @param sourceDir table location
     * @return table
     */
    public static Table readTable(@NotNull final String sourceDir) {
        return readTable(new File(sourceDir));
    }

    public static Table readTable(@NotNull final File sourceDir) {
        return readTableWithClassLoader(sourceDir, null);
    }

    /**
     * Reads in a table from disk.
     *
     * @param sourceDir table location
     * @param classLoader a class loader to use for encoded types, or null to use bootstrap classloader.
     * @return table
     */
    public static Table readTableWithClassLoader(@NotNull final File sourceDir, final ClassLoader classLoader) {
        final ArrayList<ColumnDefinition> cols = new ArrayList<>();
        final ParquetReaderUtil.ColumnDefinitionConsumer colConsumer =
                (final String name, final Class<?> dbType, Class<?> componentType, final boolean isGrouping, final String codecName, final String codecArgs) -> {
                    final int columnType = isGrouping ? ColumnDefinition.COLUMNTYPE_GROUPING : ColumnDefinition.COLUMNTYPE_NORMAL;
                    final ColumnDefinition<?> colDef;
                    if (codecName != null) {
                        colDef = ColumnDefinition.ofVariableWidthCodec(name, dbType, columnType, componentType, codecName, codecArgs);
                    } else {
                        colDef = ColumnDefinition.fromGenericType(name, dbType, columnType, componentType);
                    }
                    cols.add(colDef);
                };
        try {
            ParquetReaderUtil.readParquetSchema(sourceDir.getPath() + File.separator + "table.parquet", colConsumer, classLoader);
        } catch (java.io.FileNotFoundException e) {
            throw new IllegalArgumentException(sourceDir + " doesn't have a loadable TableDefinition");
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Error trying to load table definition from parquet file: " + e, e);
        }
        return readTable(sourceDir, new TableDefinition(cols));
    }

    /**
     * Write out a table to disk.
     *
     * @param sourceTable source table
     * @param destDir destination
     */
    public static void writeTable(Table sourceTable, String destDir) {
        writeTable(sourceTable, destDir, StorageFormat.Parquet);
    }

    /**
     * Write out a table to disk.
     *
     * @param sourceTable source table
     * @param destDir destination
     * @param storageFormat Format used for storage
     */
    public static void writeTable(Table sourceTable, String destDir, StorageFormat storageFormat) {
        writeTable(sourceTable, sourceTable.getDefinition(), new File(destDir), storageFormat);
    }

    /**
     * Write out a table to disk.
     *
     * @param sourceTable source table
     * @param definition table definition.  Will be written to disk as given.
     * @param destDir destination
     * @param storageFormat Format used for storage
     */
    public static void writeTable(Table sourceTable, TableDefinition definition, File destDir, StorageFormat storageFormat) {
        if (storageFormat == StorageFormat.Parquet) {
            writeParquetTables(new Table[]{sourceTable}, definition, CompressionCodecName.SNAPPY, new File[]{destDir}, definition.getGroupingColumnNamesArray());
        } else {
            throw new IllegalArgumentException("Unrecognized storage format " + storageFormat);
        }
    }

    /**
     * Write out a table to disk.
     *
     * @param sourceTable source table
     * @param destDir destination
     */
    public static void writeTable(Table sourceTable, File destDir) {
        writeTable(sourceTable, destDir, StorageFormat.Parquet);
    }

    /**
     * Write out a table to disk.
     *
     * @param sourceTable source table
     * @param destDir destination
     * @param storageFormat Format used for storage
     */
    public static void writeTable(Table sourceTable, File destDir, StorageFormat storageFormat) {
        writeTable(sourceTable, sourceTable.getDefinition(), destDir, storageFormat);
    }

    /**
     * Delete the destination directory if it exists, then make the destination directory and any missing parent
     * directories, returning the first created for later rollback.
     *
     * @param destination The destination directory
     * @return The first created directory
     */
    private static File prepareDestination(@NotNull File destination) {
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

    /**
     * Writes tables to disk in parquet format under a given destinations.  If you specify grouping columns, there
     * must already be grouping information for those columns in the sources.  This can be accomplished with
     * {@code .by(<grouping columns>).ungroup()} or {@code .sort(<grouping column>)}.
     *
     * @param sources         The tables to write
     * @param tableDefinition The common schema for all the tables to write
     * @param codecName       Compression codec to use.  The only supported codecs are
     *                        {@link CompressionCodecName#SNAPPY} and {@link CompressionCodecName#UNCOMPRESSED}.
     *
     * @param destinations    The destinations path
     * @param groupingColumns List of columns the tables are grouped by (the write operation will store the grouping info)
     */
    public static void writeParquetTables(@NotNull final Table[] sources,
                                          @NotNull final TableDefinition tableDefinition,
                                          CompressionCodecName codecName,
                                          @NotNull final File[] destinations, String[] groupingColumns) {
        Require.eq(sources.length, "sources.length", destinations.length, "destinations.length");
        final File[] absoluteDestinations = Arrays.stream(destinations).map(File::getAbsoluteFile).toArray(File[]::new);
        final File[] firstCreatedDirs = Arrays.stream(absoluteDestinations).map(TableManagementTools::prepareDestination).toArray(File[]::new);

        for (int i = 0; i < sources.length; i++) {
            Table source = sources[i];
            try {
                String path = destinations[i].getPath();
                ParquetTableWriter.write(source, path + File.separator + "table.parquet", Collections.emptyMap(), codecName, tableDefinition.getWritable(),
                        c -> path + File.separator + ParquetTableWriter.defaultGroupingFileName.apply(c), groupingColumns);
            } catch (Exception e) {
                log.error("Error in table writing, cleaning up potentially incomplete table destination path up to " +
                        destinations[i].getAbsolutePath(), e);
                for (final File firstCreatedDir : firstCreatedDirs) {
                    log.error("Error in table writing, cleaning up potentially incomplete table destination path starting from " +
                            firstCreatedDir.getAbsolutePath(), e);
                    FileUtils.deleteRecursivelyOnNFS(firstCreatedDir);
                }
                throw new RuntimeException("Error in table writing", e);
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
        writeTables(sources, tableDefinition, destinations, StorageFormat.Parquet);
    }

    /**
     * Write out tables to disk.
     *
     * @param sources source tables
     * @param tableDefinition table definition
     * @param destinations destinations
     * @param storageFormat Format used for storage
     */
    public static void writeTables(@NotNull final Table[] sources,
                                   @NotNull final TableDefinition tableDefinition,
                                   @NotNull final File[] destinations,
                                   StorageFormat storageFormat) {
        if (StorageFormat.Parquet == storageFormat) {
            writeParquetTables(sources, tableDefinition, CompressionCodecName.SNAPPY, destinations,
                    tableDefinition.getGroupingColumnNamesArray());
        } else {
            throw new IllegalArgumentException("Unrecognized storage format " + storageFormat);
        }
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
