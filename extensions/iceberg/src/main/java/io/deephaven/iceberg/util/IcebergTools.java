//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.util.internal.PropertyAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Tools for accessing tables in the Iceberg table format.
 */
public abstract class IcebergTools {
    @SuppressWarnings("unused")
    public static IcebergCatalogAdapter createAdapter(
            final Catalog catalog,
            final FileIO fileIO) {
        return new IcebergCatalogAdapter(catalog, fileIO);
    }

    public static Table readStatic(
            String metadataFileLocation,
            IcebergInstructions instructions,
            Map<String, String> properties,
            Map<String, String> hadoopProperties,
            boolean adaptProperties) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        if (instructions != null && adaptProperties) {
            final Object dataInstructions = instructions.dataInstructions().orElse(null);
            if (dataInstructions != null) {
                PropertyAdapter.serviceLoaderAdapt(dataInstructions, properties);
            }
        }
        final Configuration hadoopConf = new Configuration();
        if (hadoopProperties != null) {
            for (Entry<String, String> e : hadoopProperties.entrySet()) {
                hadoopConf.set(e.getKey(), e.getValue());
            }
        }
        return readStatic(metadataFileLocation, instructions, properties, hadoopConf);
    }

    public static Table readStatic(
            String metadataFileLocation,
            IcebergInstructions instructions,
            Map<String, String> properties,
            Configuration hadoopConf) {
        // final HadoopTables tables = new HadoopTables(hadoopConf);
        // final org.apache.iceberg.Table table = tables.load(uri);
        final FileIO fileIO = CatalogUtil.loadFileIO(ResolvingFileIO.class.getName(), properties, hadoopConf);
        final BaseTable table =
                new BaseTable(new StaticTableOperations(metadataFileLocation, fileIO), metadataFileLocation);
        final Snapshot snapshot = table.currentSnapshot();
        final Schema schema = snapshot == null ? table.schema() : table.schemas().get(snapshot.schemaId());
        return snapshot == null
                ? IcebergCatalogAdapter.readTableNoSnapshot(schema, table, instructions)
                : IcebergCatalogAdapter.readTableSnapshot(schema, table, instructions, table.currentSnapshot(), fileIO);
    }

    public static TableDefinition readStaticDefinition(
            String metadataFileLocation,
            IcebergInstructions instructions,
            Map<String, String> properties,
            Map<String, String> hadoopProperties,
            boolean adaptProperties) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        if (instructions != null && adaptProperties) {
            final Object dataInstructions = instructions.dataInstructions().orElse(null);
            if (dataInstructions != null) {
                PropertyAdapter.serviceLoaderAdapt(dataInstructions, properties);
            }
        }
        final Configuration hadoopConf = new Configuration();
        if (hadoopProperties != null) {
            for (Entry<String, String> e : hadoopProperties.entrySet()) {
                hadoopConf.set(e.getKey(), e.getValue());
            }
        }
        return readStaticDefinition(metadataFileLocation, instructions, properties, hadoopConf);
    }

    public static TableDefinition readStaticDefinition(
            String metadataFileLocation,
            IcebergInstructions instructions,
            Map<String, String> properties,
            Configuration hadoopConf) {
        // final HadoopTables tables = new HadoopTables(hadoopConf);
        // final org.apache.iceberg.Table table = tables.load(uri);
        final FileIO fileIO = CatalogUtil.loadFileIO(ResolvingFileIO.class.getName(), properties, hadoopConf);
        final BaseTable table =
                new BaseTable(new StaticTableOperations(metadataFileLocation, fileIO), metadataFileLocation);
        final Snapshot snapshot = table.currentSnapshot();
        final Schema schema = snapshot == null ? table.schema() : table.schemas().get(snapshot.schemaId());
        return IcebergCatalogAdapter.tableDefinition(schema, table, instructions,
                snapshot == null ? -1 : snapshot.snapshotId());
    }
}
