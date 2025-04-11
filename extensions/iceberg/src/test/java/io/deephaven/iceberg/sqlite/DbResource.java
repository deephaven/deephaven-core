//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.sqlite;

import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTools;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.FileUtils;

public class DbResource {

    /**
     * Note: we are using a resource path that is not shared with our build classes to ensure we are resolving against
     * the actual resource directory
     */
    private static final Path RESOURCE_DIR =
            Path.of(Objects.requireNonNull(DbResource.class.getResource("db_resource")).getFile());

    /**
     * Open a catalog using the default resource directory. This should only be used when the resources are not
     * modified. For example, when reading from tables.
     */
    public static IcebergCatalogAdapter openCatalog(final String catalogName) {
        return openCatalogImpl(catalogName, RESOURCE_DIR);
    }

    /**
     * Open a catalog using the provided root directory. This will copy the resources from {@link #RESOURCE_DIR} to the
     * {@code rootDir}. This method useful for creating a catalog when running tests which modify the resources, like
     * writing to a table.
     */
    public static IcebergCatalogAdapter openCatalog(final String catalogName, final Path rootDir) throws IOException {
        FileUtils.copyDirectory(RESOURCE_DIR.toFile(), rootDir.toFile());
        return openCatalogImpl(catalogName, rootDir);
    }

    private static IcebergCatalogAdapter openCatalogImpl(final String catalogName, final Path rootDir) {
        final Map<String, String> properties = new HashMap<>();
        SqliteHelper.setJdbcCatalogProperties(properties, rootDir);
        SqliteHelper.setLocalFileIoProperties(properties, rootDir, catalogName, true);
        // Note: the catalogName is very important here, the JDBC catalog uses it for lookups. In this way, a single
        // dbFile can be used for multiple catalogs.
        return IcebergTools.createAdapter(catalogName, properties);
    }
}
