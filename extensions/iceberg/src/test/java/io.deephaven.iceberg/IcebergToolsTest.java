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
import java.time.Instant;
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
        asyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).get();

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
    }

    private void uploadParquetFiles(final File root, final String prefixToRemove)
            throws ExecutionException, InterruptedException, TimeoutException {
        for (final File file : root.listFiles()) {
            if (file.isDirectory()) {
                uploadParquetFiles(file, prefixToRemove);
            } else if (file.getName().endsWith(".parquet")) {
                final String key = file.getPath().substring(prefixToRemove.length() + 1);

                keys.add(key);
                final CompletableFuture<PutObjectResponse> future = asyncClient.putObject(
                        PutObjectRequest.builder().bucket(bucket).key(key).build(),
                        AsyncRequestBody.fromFile(file));

                final PutObjectResponse response = future.get(10, TimeUnit.SECONDS);
                if (!response.sdkHttpResponse().isSuccessful()) {
                    Assert.statementNeverExecuted("Failed to upload file: " + file.getPath());
                }
            }
        }
    }

    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException {
        for (String key : keys) {
            asyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build()).get();
        }
        keys.clear();
        asyncClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()).get();
        asyncClient.close();
    }

    @Test
    public void testListTables() {
        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Namespace ns = Namespace.of("sales");

        final Collection<TableIdentifier> tables = adapter.listTables(ns);
        Assert.eq(tables.size(), "tables.size()", 3, "3 tables in the namespace");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_multi")), "tables.contains(sales_multi)");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_partitioned")),
                "tables.contains(sales_partitioned)");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_single")), "tables.contains(sales_single)");
    }

    @Test
    public void testListTableSnapshots() {
        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Collection<Long> snapshots = adapter.listTableSnapshots(TableIdentifier.of("sales", "sales_multi"));

        Assert.eq(snapshots.size(), "snapshots.size()", 4, "4 snapshots for sales/sales_multi");

        Assert.eqTrue(snapshots.contains(2001582482032951248L), "snapshots.contains(2001582482032951248)");
        Assert.eqTrue(snapshots.contains(8325605756612719366L), "snapshots.contains(8325605756612719366L)");
        Assert.eqTrue(snapshots.contains(3247344357341484163L), "snapshots.contains(3247344357341484163L)");
        Assert.eqTrue(snapshots.contains(1792185872197984875L), "snapshots.contains(1792185872197984875L)");
    }

    @Test
    public void testOpenTableA() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableB() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_multi").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_multi");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableC() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_single").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructionsS3Only);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_single");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableS3Only() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructions);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableDefinition() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("year").withPartitioning(),
                ColumnDefinition.ofInt("month").withPartitioning(),
                ColumnDefinition.ofString("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.fromGenericType("Order_Date", Instant.class));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTablePartitionTypeException() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofLong("year").withPartitioning(),
                ColumnDefinition.ofInt("month").withPartitioning(),
                ColumnDefinition.ofLong("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofDouble("Units_Sold"),
                ColumnDefinition.ofLong("Unit_Price"),
                ColumnDefinition.fromGenericType("Order_Date", Instant.class));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        try {
            final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);
            TableTools.showWithRowSet(table, 100, DateTimeUtils.timeZone(), System.out);
            Assert.statementNeverExecuted("Expected an exception for missing columns");
        } catch (final TableDefinition.IncompatibleTableDefinitionException e) {
            Assert.eqTrue(e.getMessage().startsWith("Table definition incompatibilities"), "Exception message");
        }
    }

    @Test
    public void testOpenTableDefinitionRename() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("__year").withPartitioning(),
                ColumnDefinition.ofInt("__month").withPartitioning(),
                ColumnDefinition.ofString("RegionName"),
                ColumnDefinition.ofString("ItemType"),
                ColumnDefinition.ofInt("UnitsSold"),
                ColumnDefinition.ofDouble("UnitPrice"),
                ColumnDefinition.fromGenericType("OrderDate", Instant.class));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .putColumnRenameMap("Region", "RegionName")
                .putColumnRenameMap("Item_Type", "ItemType")
                .putColumnRenameMap("Units_Sold", "UnitsSold")
                .putColumnRenameMap("Unit_Price", "UnitPrice")
                .putColumnRenameMap("Order_Date", "OrderDate")
                .putColumnRenameMap("year", "__year")
                .putColumnRenameMap("month", "__month")
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testMissingPartitioningColumns() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("__year").withPartitioning(), // Incorrect name
                ColumnDefinition.ofInt("__month").withPartitioning(), // Incorrect name
                ColumnDefinition.ofLong("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofDouble("Units_Sold"),
                ColumnDefinition.ofLong("Unit_Price"),
                ColumnDefinition.fromGenericType("Order_Date", Instant.class));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        try {
            final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);
            Assert.statementNeverExecuted("Expected an exception for missing columns");
        } catch (final TableDefinition.IncompatibleTableDefinitionException e) {
            Assert.eqTrue(e.getMessage().startsWith("Table definition incompatibilities"), "Exception message");
        }
    }

    @Test
    public void testOpenTableColumnRename() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .putColumnRenameMap("RegionName", "Region")
                .putColumnRenameMap("ItemType", "Item_Type")
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableColumnRenamePartitioningColumns()
            throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .s3Instructions(instructionsS3Only.s3Instructions().get())
                .putColumnRenameMap("VendorID", "vendor_id")
                .putColumnRenameMap("month", "__month")
                .putColumnRenameMap("year", "__year")
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO, localInstructions);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.snapshotTable(tableId);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableSnapshot() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_multi").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO, instructions);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_multi");
        final List<Long> snapshots = adapter.listTableSnapshots(tableId);

        // Verify we retrieved all the rows.
        final io.deephaven.engine.table.Table table0 = adapter.snapshotTable(tableId, snapshots.get(0));
        Assert.eq(table0.size(), "table0.size()", 18266, "18266 rows in the table");

        final io.deephaven.engine.table.Table table1 = adapter.snapshotTable(tableId, snapshots.get(1));
        Assert.eq(table1.size(), "table1.size()", 54373, "54373 rows in the table");

        final io.deephaven.engine.table.Table table2 = adapter.snapshotTable(tableId, snapshots.get(2));
        Assert.eq(table2.size(), "table2.size()", 72603, "72603 rows in the table");

        final io.deephaven.engine.table.Table table3 = adapter.snapshotTable(tableId, snapshots.get(3));
        Assert.eq(table3.size(), "table3.size()", 100_000, "100_000 rows in the table");
    }
}
