//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.sqlite;

import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTools;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DbResource {
    public static IcebergCatalogAdapter openCatalog(String catalogName) throws URISyntaxException {
        // Note: we are using a resource path that is not shared with our build classes to ensure we are
        // resolving against the actual resource directory
        final Path rootDir = Path.of(DbResource.class.getResource("db_resource").toURI());
        final Map<String, String> properties = new HashMap<>();
        SqliteHelper.setJdbcCatalogProperties(properties, rootDir);
        SqliteHelper.setLocalFileIoProperties(properties, rootDir, catalogName, true);
        // Note: the catalogName is very important here, the JDBC catalog uses it for lookups. In this way, a single
        // dbFile can be used for multiple catalogs.
        return IcebergTools.createAdapter(catalogName, properties);
    }
}
