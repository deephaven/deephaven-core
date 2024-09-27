//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.sqlite;

import io.deephaven.iceberg.relative.RelativeFileIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class SqliteHelper {

    private static final String DB_FILE = "dh-iceberg-test.db";
    private static final String CATALOGS_DIR = "catalogs";

    public static void createJdbcDatabase(Path rootDir) throws IOException {
        if (!Files.isDirectory(rootDir)) {
            throw new IllegalArgumentException("Must provide rootDir that exists: " + rootDir);
        }
        try (final Stream<Path> list = Files.list(rootDir).limit(1)) {
            if (list.iterator().hasNext()) {
                throw new IllegalArgumentException("Expected rootDir to be empty: " + rootDir);
            }
        }
        Files.createFile(rootDir.resolve(DB_FILE));
        Files.createDirectory(rootDir.resolve("catalogs"));
    }

    public static Catalog createJdbcCatalog(Path rootDir, String catalogName, boolean relativeSupport)
            throws IOException {
        if (!Files.isDirectory(rootDir)) {
            throw new IllegalArgumentException("Must provide rootDir that exists: " + rootDir);
        }
        if (!Files.isRegularFile(rootDir.resolve(DB_FILE))) {
            throw new IllegalArgumentException("Must create jdbc database first: " + rootDir);
        }
        if (!Files.isDirectory(rootDir.resolve(CATALOGS_DIR))) {
            throw new IllegalArgumentException("Must create jdbc database first: " + rootDir);
        }
        Files.createDirectory(rootDir.resolve(CATALOGS_DIR).resolve(catalogName));
        return openJdbcCatalog(rootDir, catalogName, relativeSupport);
    }

    public static Catalog openJdbcCatalog(Path rootDir, String catalogName, boolean relativeSupport) {
        final Path warehouseDir = rootDir.resolve(CATALOGS_DIR).resolve(catalogName);
        if (!Files.isDirectory(warehouseDir)) {
            throw new IllegalArgumentException("Expected warehouse to already exist: " + warehouseDir);
        }
        final Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        properties.put(CatalogProperties.URI, String.format("jdbc:sqlite:%s", rootDir.resolve(DB_FILE)));
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseDir.toString());
        if (relativeSupport) {
            // When we are referring to a catalog that already exists in our unit testing filesystem, we need to make
            // hook in relative file support. See https://github.com/apache/iceberg/issues/1617
            properties.put(CatalogProperties.FILE_IO_IMPL, RelativeFileIO.class.getName());
            properties.put(RelativeFileIO.BASE_PATH, rootDir.toString());
        }
        final Configuration hadoopConf = new Configuration();
        // Note: the catalogName is very important here, the JDBC catalog uses it for lookups. In this way, a single
        // dbFile can be used for multiple catalogs.
        return CatalogUtil.buildIcebergCatalog(catalogName, properties, hadoopConf);
    }
}
