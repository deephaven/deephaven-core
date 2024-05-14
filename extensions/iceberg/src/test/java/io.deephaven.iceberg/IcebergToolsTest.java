//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.TestCatalog.IcebergTestCatalog;
import io.deephaven.iceberg.TestCatalog.IcebergTestFileIO;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergInstructions;
import io.deephaven.iceberg.util.IcebergTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.time.DateTimeUtils;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class IcebergToolsTest {
    IcebergInstructions instructions;
    IcebergInstructions instructionsS3Only;

    public abstract S3AsyncClient s3AsyncClient();

    public abstract S3Instructions.Builder s3Instructions(S3Instructions.Builder builder);

    private S3AsyncClient asyncClient;
    private String bucket;

    private final List<String> keys = new ArrayList<>();

    private String warehousePath;
    private Catalog resourceCatalog;
    private FileIO resourceFileIO;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        bucket = "warehouse";
        asyncClient = s3AsyncClient();
        final CompletableFuture<CreateBucketResponse> bucketCreated =
                asyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

        warehousePath = IcebergToolsTest.class.getResource("/warehouse").getPath();
        resourceFileIO = new IcebergTestFileIO("s3://warehouse", warehousePath);

        // Create the test catalog for the tests
        resourceCatalog = IcebergTestCatalog.create(warehousePath, resourceFileIO);

        final S3Instructions s3Instructions = s3Instructions(S3Instructions.builder()).build();

        final ParquetInstructions parquetInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions)
                .build();

        instructionsS3Only = IcebergInstructions.builder()
                .s3Instructions(s3Instructions)
                .build();
        instructions = IcebergInstructions.builder()
                .parquetInstructions(parquetInstructions)
                .build();

        bucketCreated.get();
    }

    private void uploadParquetFiles(final File root, final String prefixToRemove)
            throws ExecutionException, InterruptedException, TimeoutException {
        for (final File file : root.listFiles()) {
            if (file.isDirectory()) {
                uploadParquetFiles(file, prefixToRemove);
            } else if (file.getName().endsWith(".parquet")) {
                final String key = file.getPath().substring(prefixToRemove.length() + 1);
                keys.add(key);
                putObject(key, AsyncRequestBody.fromFile(file));
            }
        }
    }

    @AfterEach
    public void tearDown() {
        for (String key : keys) {
            asyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        }
        keys.clear();
        asyncClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
        asyncClient.close();
    }

    @Test
    public void testNothing() {
        // Dummy to prevent JUnit from complaining about no tests
    }

    // TODO: discuss how to perform tests since they require a full MiniIO + Iceberg setup

    @Test
    public void testListTables() {
        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Namespace ns = Namespace.of("nyc");

        final Collection<TableIdentifier> tables = adapter.listTables(ns);
        Assert.eq(tables.size(), "tables.size()", 3, "3 tables in the namespace");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "taxis")), "tables.contains(nyc/taxis)");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "taxis_partitioned")),
                "tables.contains(nyc/taxis_partitioned)");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "taxis_single")), "tables.contains(nyc/taxis_single)");
    }

    @Test
    public void testListTableSnapshots() {
        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Collection<Long> snapshots = adapter.listTableSnapshots(TableIdentifier.of("nyc", "taxis"));

        Assert.eq(snapshots.size(), "snapshots.size()", 4, "4 snapshots for nyc/taxis");

        Assert.eqTrue(snapshots.contains(7480254251893511044L), "snapshots.contains(7480254251893511044L)");
        Assert.eqTrue(snapshots.contains(4768271945146524109L), "snapshots.contains(4768271945146524109L)");
        Assert.eqTrue(snapshots.contains(7258036030029852722L), "snapshots.contains(7258036030029852722L)");
        Assert.eqTrue(snapshots.contains(615105126920399770L), "snapshots.contains(615105126920399770L)");
    }

    @Test
    public void testOpenTableA() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/nyc/taxis_partitioned").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    @Test
    public void testOpenTableB() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/nyc/taxis").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    @Test
    public void testOpenTableC() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/nyc/taxis_single").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_single");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    @Test
    public void testOpenTableTestCatalog() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/nyc/taxis_single").getPath()),
                warehousePath);
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_single");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    @Test
    public void testOpenTableS3Only() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/nyc/taxis_partitioned").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    @Test
    public void testOpenTableDefinition() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/nyc/taxis_partitioned").getPath()),
                warehousePath);

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

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

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

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        try {
            final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);
            TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
            Assert.statementNeverExecuted("Expected an exception for missing columns");
        } catch (final TableDefinition.IncompatibleTableDefinitionException e) {
            Assert.eqTrue(e.getMessage().startsWith("Table definition incompatibilities"), "Exception message");
        }
    }

    @Test
    public void testOpenTableDefinitionRename() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("__year").withPartitioning(),
                ColumnDefinition.ofInt("__month").withPartitioning(),
                ColumnDefinition.ofLong("vendor_id"),
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

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

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

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        try {
            final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);
            Assert.statementNeverExecuted("Expected an exception for missing columns");
        } catch (final TableDefinition.IncompatibleTableDefinitionException e) {
            Assert.eqTrue(e.getMessage().startsWith("Table definition incompatibilities"), "Exception message");
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

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

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

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    @Test
    public void testOpenTableSnapshot() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_partitioned");
        final List<Long> snapshots = adapter.listTableSnapshots(tableId);

        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId, snapshots.get(0));

        TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
    }

    private void putObject(String key, AsyncRequestBody body)
            throws ExecutionException, InterruptedException, TimeoutException {
        asyncClient.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), body).get(5,
                TimeUnit.SECONDS);
    }
}
