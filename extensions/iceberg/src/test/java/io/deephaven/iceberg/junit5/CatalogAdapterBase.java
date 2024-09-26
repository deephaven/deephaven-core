//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.junit5;

import io.deephaven.iceberg.CatalogHelper;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTools;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;


public abstract class CatalogAdapterBase {

    protected IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp(TestInfo testInfo, @TempDir Path catalogDir) throws IOException {
        catalogAdapter =
                IcebergTools
                        .createAdapter(CatalogHelper.createJdbcCatalog(testInfo.getDisplayName(), catalogDir, true));
    }
}
