//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.sqlite;

import io.deephaven.iceberg.relative.RelativeFileIO;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;

import java.nio.file.Path;
import java.util.Map;

public class SqliteHelper {

    private static final String DB_FILE = "dh-iceberg-test.db";
    private static final String CATALOGS_DIR = "catalogs";

    public static void setJdbcCatalogProperties(Map<String, String> props, Path rootDir) {
        put(props, CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        put(props, CatalogProperties.URI, String.format("jdbc:sqlite:%s", rootDir.resolve(DB_FILE)));
    }

    public static void setLocalFileIoProperties(
            Map<String, String> properties, Path rootDir, String catalogName, boolean relativeSupport) {
        final Path warehouseDir = rootDir.resolve(CATALOGS_DIR).resolve(catalogName);
        put(properties, CatalogProperties.WAREHOUSE_LOCATION, warehouseDir.toString());
        if (relativeSupport) {
            // When we are referring to a catalog that already exists in our unit testing filesystem, we need to make
            // hook in relative file support. See https://github.com/apache/iceberg/issues/1617
            put(properties, CatalogProperties.FILE_IO_IMPL, RelativeFileIO.class.getName());
            put(properties, RelativeFileIO.BASE_PATH, rootDir.toString());
            put(properties, RelativeFileIO.IO_IMPL, HadoopFileIO.class.getName());
        } else {
            put(properties, CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName());
        }
    }

    private static <K, V> void put(Map<K, V> map, K key, V value) {
        if (map.putIfAbsent(key, value) != null) {
            throw new IllegalStateException(String.format("Key '%s' already exists in map", key));
        }
    }
}
