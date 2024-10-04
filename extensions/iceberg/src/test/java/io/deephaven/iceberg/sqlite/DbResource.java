//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.sqlite;

import org.apache.iceberg.catalog.Catalog;

import java.net.URISyntaxException;
import java.nio.file.Path;

public class DbResource {
    public static Catalog openCatalog(String catalogName) throws URISyntaxException {
        return SqliteHelper.openJdbcCatalog(
                // Note: we are using a resource path that is not shared with our build classes to ensure we are
                // resolving against the actual resource directory
                Path.of(DbResource.class.getResource("db_resource").toURI()),
                catalogName,
                true);
    }
}
