//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.junit5;

import io.deephaven.iceberg.sqlite.SqliteHelper;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTools;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

import java.nio.file.Path;
import java.util.Map;

/**
 * This is a test base that allows the complete local testing of a Catalog via a JDBC catalog driver, with files being
 * served via local file IO.
 */
@Tag("security-manager-allow")
final class FileWarehouseSqliteCatalogTest extends SqliteCatalogBase {

    @Override
    @Nullable
    public Object dataInstructions() {
        return null;
    }

    @Override
    protected IcebergCatalogAdapter catalogAdapter(TestInfo testInfo, Path rootDir, Map<String, String> properties) {
        final String catalogName = testInfo.getTestMethod().orElseThrow().getName() + "-catalog";;
        // no relative support needed, we don't need this data to be persistent / portable
        SqliteHelper.setLocalFileIoProperties(properties, rootDir, catalogName, false);
        return IcebergTools.createAdapter(catalogName, properties);
    }
}
