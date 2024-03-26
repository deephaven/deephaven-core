/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.iceberg;

import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.util.IcebergCatalog;
import io.deephaven.iceberg.util.IcebergInstructions;
import io.deephaven.iceberg.util.IcebergTools;
import io.deephaven.time.DateTimeUtils;
import junit.framework.TestCase;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Test;

import java.util.List;

public class IcebergToolsTest extends TestCase {
    IcebergInstructions instructions;

    @Override
    public void setUp() {
        instructions = IcebergInstructions.builder()
                .catalogImpl(IcebergInstructions.CATALOG_IMPL.RESTCatalog)
                .catalogURI("http://rest:8181")
                .warehouseLocation("s3a://warehouse/wh")
                .fileIOImpl(IcebergInstructions.FILEIO_IMPL.S3)
                .s3AccessKeyId("admin")
                .s3SecretAccessKey("password")
                .s3EndpointOverride("http://minio:9000")
                .s3Region("us-east-1")
                .build();
    }

    @Test
    public void testListTables() {
        final IcebergCatalog catalog = IcebergTools.createCatalog("minio-iceberg", instructions);

        final Namespace ns = Namespace.of("nyc");
        final List<TableIdentifier> tables = catalog.listTables(ns);
    }

    @Test
    public void testOpenTable() {
        final IcebergCatalog catalog = IcebergTools.createCatalog("minio-iceberg", instructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        io.deephaven.engine.table.Table table = catalog.readTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

}
