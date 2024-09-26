//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

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

public class CatalogHelper {

    // Using specific names to make clear these aren't a standard / convention
    public static final String CATALOG_NAME = "dh-test-catalog.db";
    public static final String WAREHOUSE_NAME = "dh-test-warehouse";

    public static Catalog createJdbcCatalog(String name, Path path, boolean isNew) throws IOException {
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException("Must provide directory that exists");
        }
        if (isNew) {
            try (final Stream<Path> list = Files.list(path).limit(1)) {
                if (list.iterator().hasNext()) {
                    throw new IllegalArgumentException("Expected directory to be empty");
                }
            }
        }
        final Path catalogFile = path.resolve(CATALOG_NAME);
        final Path warehouseDir = path.resolve(WAREHOUSE_NAME);
        if (isNew) {
            Files.createDirectory(warehouseDir);
        } else {
            if (!Files.isDirectory(warehouseDir)) {
                throw new IllegalStateException("Expected warehouse directory to already exist");
            }
        }
        final Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        properties.put(CatalogProperties.URI, String.format("jdbc:sqlite:%s", catalogFile));
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseDir.toString());
        if (!isNew) {
            // When we are referring to a catalog that already exists in our unit testing filesystem, we need to make
            // hook in relative file support. See https://github.com/apache/iceberg/issues/1617
            properties.put(CatalogProperties.FILE_IO_IMPL, RelativeFileIO.class.getName());
            properties.put(RelativeFileIO.BASE_PATH, path.toString());
        }
        final Configuration hadoopConf = new Configuration();
        return CatalogUtil.buildIcebergCatalog(name, properties, hadoopConf);
    }
}
