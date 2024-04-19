//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
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
    public void testOpenTableS3Only() {
        final IcebergCatalogAdapter adapter = IcebergTools.createS3Rest(
                "minio-iceberg",
                "http://rest:8181",
                "s3a://warehouse/wh",
                "us-east-1",
                "admin",
                "password",
                "http://minio:9000",
                instructionsS3Only);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    @Test
    public void testOpenTableDefinition() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("year").withPartitioning(),
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
    public void testOpenTablePartitionTypeException() {
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
        try {
            final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);
            TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
            Assert.statementNeverExecuted("Expected an exception for missing columns");
        } catch (final Exception e) {
            Assert.eqTrue(e instanceof TableDataException, "Exception type");
        }
    }

    @Test
    public void testOpenTableDefinitionRename() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("__year").withPartitioning(),
                ColumnDefinition.ofInt("__month").withPartitioning(),
                ColumnDefinition.ofLong("VendorID"),
                ColumnDefinition.fromGenericType("pickup_datetime", LocalDateTime.class),
                ColumnDefinition.fromGenericType("dropoff_datetime", LocalDateTime.class),
                ColumnDefinition.ofDouble("passenger_count"));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .putColumnRenameMap("VendorID", "vendor_id")
                .putColumnRenameMap("tpep_pickup_datetime", "pickup_datetime")
                .putColumnRenameMap("tpep_dropoff_datetime", "dropoff_datetime")
                .putColumnRenameMap("year", "__year")
                .putColumnRenameMap("month", "__month")
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
    public void testMissingPartitioningColumns() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("__year").withPartitioning(), // Incorrect name
                ColumnDefinition.ofInt("__month").withPartitioning(), // Incorrect name
                ColumnDefinition.ofLong("VendorID"),
                ColumnDefinition.fromGenericType("pickup_datetime", LocalDateTime.class),
                ColumnDefinition.fromGenericType("dropoff_datetime", LocalDateTime.class),
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
        try {
            final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);
            Assert.statementNeverExecuted("Expected an exception for missing columns");
        } catch (final IllegalStateException e) {
            Assert.eqTrue(e.getMessage().startsWith("Partitioning column(s)"), "Exception message");
            Assert.eqTrue(e.getMessage().contains("were not found in the table definition"), "Exception message");
        }
    }

    @Test
    public void testOpenTableColumnRename() {
        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .putColumnRenameMap("VendorID", "vendor_id")
                .putColumnRenameMap("tpep_pickup_datetime", "pickup_datetime")
                .putColumnRenameMap("tpep_dropoff_datetime", "dropoff_datetime")
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
    public void testOpenTableColumnRenamePartitioningColumns() {
        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .putColumnRenameMap("VendorID", "vendor_id")
                .putColumnRenameMap("month", "__month")
                .putColumnRenameMap("year", "__year")
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

    @Test
    public void testOpenAllTypesTable() {
        final IcebergCatalogAdapter adapter = IcebergTools.createS3Rest(
                "minio-iceberg",
                "http://rest:8181",
                "s3a://warehouse/wh",
                "us-east-1",
                "admin",
                "password",
                "http://minio:9000",
                instructions);
        final Namespace ns = Namespace.of("sample");
        final TableIdentifier tableId = TableIdentifier.of(ns, "all_types");
        io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }
}
