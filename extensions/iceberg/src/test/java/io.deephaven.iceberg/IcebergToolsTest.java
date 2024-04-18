//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergInstructions;
import io.deephaven.iceberg.util.IcebergTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.time.DateTimeUtils;
import junit.framework.TestCase;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

public class IcebergToolsTest extends TestCase {
    IcebergInstructions instructions;
    IcebergInstructions instructionsS3Only;

    @Override
    public void setUp() {
        final S3Instructions s3Instructions = S3Instructions.builder()
                .credentials(Credentials.basic("admin", "password"))
                .endpointOverride("http://minio:9000")
                .regionName("us-east-1")
                .build();
        final ParquetInstructions parquetInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions)
                .build();

        instructionsS3Only = IcebergInstructions.builder()
                .s3Instructions(s3Instructions)
                .build();
        instructions = IcebergInstructions.builder()
                .parquetInstructions(parquetInstructions)
                .build();
    }

    @Test
    public void testNothing() {
        // Dummy to prevent JUnit from complaining about no tests
    }

    // TODO: discuss how to perform tests since they require a full MiniIO + Iceberg setup

    @Test
    public void testListTables() {
        final IcebergCatalogAdapter adapter = IcebergTools.createS3Rest(
                "minio-iceberg",
                "http://rest:8181",
                "s3a://warehouse/wh",
                "us-east-1",
                "admin",
                "password",
                "http://minio:9000",
                instructions);

        final Namespace ns = Namespace.of("nyc");
        final Collection<TableIdentifier> tables = adapter.listTables(ns);
    }


    @Test
    public void testListTableSnapshots() {
        final IcebergCatalogAdapter adapter = IcebergTools.createS3Rest(
                "minio-iceberg",
                "http://rest:8181",
                "s3a://warehouse/wh",
                "us-east-1",
                "admin",
                "password",
                "http://minio:9000",
                instructions);
        final Collection<Long> snapshots = adapter.listTableSnapshots(TableIdentifier.of("nyc", "taxis_partitioned"));
    }

    @Test
    public void testOpenTable() {
        final IcebergCatalogAdapter adapter = IcebergTools.createS3Rest(
                "minio-iceberg",
                "http://rest:8181",
                "s3a://warehouse/wh",
                "us-east-1",
                "admin",
                "password",
                "http://minio:9000",
                instructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    @Test
    public void testOpenTableDefinition() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofLong("year").withPartitioning(),
                ColumnDefinition.ofInt("month").withPartitioning(),
                ColumnDefinition.ofLong("VendorID"),
                ColumnDefinition.fromGenericType("tpep_pickup_datetime", LocalDateTime.class),
                ColumnDefinition.fromGenericType("tpep_dropoff_datetime", LocalDateTime.class),
                ColumnDefinition.ofDouble("passenger_count"));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createS3Rest(
                "minio-iceberg",
                "http://rest:8181",
                "s3a://warehouse/wh",
                "us-east-1",
                "admin",
                "password",
                "http://minio:9000",
                localInstructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    @Test
    public void testOpenTableSnapshot() {
        final IcebergCatalogAdapter adapter = IcebergTools.createS3Rest(
                "minio-iceberg",
                "http://rest:8181",
                "s3a://warehouse/wh",
                "us-east-1",
                "admin",
                "password",
                "http://minio:9000",
                instructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        final List<Long> snapshots = adapter.listTableSnapshots(tableId);

        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId, snapshots.get(0));

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    // @Test
    // public void testOpenAllTypesTable() {
    // final IcebergCatalogAdapter catalog = IcebergTools.loadCatalog("minio-iceberg", instructions);
    //
    // final Namespace ns = Namespace.of("sample");
    // final TableIdentifier tableId = TableIdentifier.of(ns, "all_types");
    // io.deephaven.engine.table.Table table = catalog.readTable(tableId);
    //
    // TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    // }
}
