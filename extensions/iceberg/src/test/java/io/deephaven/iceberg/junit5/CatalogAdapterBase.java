//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.junit5;

import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.iceberg.sqlite.SqliteHelper;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTools;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

@Tag("security-manager-allow")
public abstract class CatalogAdapterBase {

    protected IcebergCatalogAdapter catalogAdapter;
    private EngineCleanup engineCleanup = new EngineCleanup();

    @BeforeEach
    void setUp(TestInfo testInfo, @TempDir Path rootDir) throws Exception {
        engineCleanup.setUp();
        SqliteHelper.createJdbcDatabase(rootDir);
        final Catalog catalog = SqliteHelper.createJdbcCatalog(rootDir, testInfo.getDisplayName(), false);
        catalogAdapter = IcebergTools.createAdapter(catalog);
    }

    @AfterEach
    void tearDown() throws Exception {
        engineCleanup.tearDown();
    }
}
