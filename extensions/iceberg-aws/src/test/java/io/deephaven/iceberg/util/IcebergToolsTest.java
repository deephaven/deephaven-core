//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.TestCatalog.IcebergTestCatalog;
import io.deephaven.iceberg.TestCatalog.IcebergTestFileIO;
import io.deephaven.time.DateTimeUtils;
import org.apache.iceberg.Snapshot;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public abstract class IcebergToolsTest {
    IcebergInstructions instructions;

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

        instructions = IcebergInstructions.builder()
                .dataInstructions(s3Instructions)
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
    public void testListNamespaces() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Collection<Namespace> namespaces = adapter.listNamespaces();
        final Collection<String> namespaceNames =
                namespaces.stream().map(Namespace::toString).collect(Collectors.toList());

        Assert.eq(namespaceNames.size(), "namespaceNames.size()", 2, "2 namespace in the catalog");
        Assert.eqTrue(namespaceNames.contains("sales"), "namespaceNames.contains(sales)");
        Assert.eqTrue(namespaceNames.contains("sample"), "namespaceNames.contains(sample)");

        final Table table = adapter.listNamespacesAsTable();
        Assert.eq(table.size(), "table.size()", 2, "2 namespace in the catalog");
        Assert.eqTrue(table.getColumnSource("namespace").getType().equals(String.class), "namespace column type");
        Assert.eqTrue(table.getColumnSource("namespace_object").getType().equals(Namespace.class),
                "namespace_object column type");
    }

    @Test
    public void testListTables() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");

        final Collection<TableIdentifier> tables = adapter.listTables(ns);
        Assert.eq(tables.size(), "tables.size()", 3, "3 tables in the namespace");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_multi")), "tables.contains(sales_multi)");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_partitioned")),
                "tables.contains(sales_partitioned)");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_single")), "tables.contains(sales_single)");

        final Table table = adapter.listTablesAsTable(ns);
        Assert.eq(table.size(), "table.size()", 3, "3 tables in the namespace");
        Assert.eqTrue(table.getColumnSource("namespace").getType().equals(String.class), "namespace column type");
        Assert.eqTrue(table.getColumnSource("table_name").getType().equals(String.class), "table_name column type");
        Assert.eqTrue(table.getColumnSource("table_identifier_object").getType().equals(TableIdentifier.class),
                "table_identifier_object column type");
    }

    @Test
    public void testListSnapshots() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final TLongArrayList snapshotIds = new TLongArrayList();
        final TableIdentifier tableIdentifier = TableIdentifier.of("sales", "sales_multi");
        adapter.listSnapshots(tableIdentifier)
                .forEach(snapshot -> snapshotIds.add(snapshot.snapshotId()));

        Assert.eq(snapshotIds.size(), "snapshots.size()", 4, "4 snapshots for sales/sales_multi");

        Assert.eqTrue(snapshotIds.contains(2001582482032951248L), "snapshots.contains(2001582482032951248)");
        Assert.eqTrue(snapshotIds.contains(8325605756612719366L), "snapshots.contains(8325605756612719366L)");
        Assert.eqTrue(snapshotIds.contains(3247344357341484163L), "snapshots.contains(3247344357341484163L)");
        Assert.eqTrue(snapshotIds.contains(1792185872197984875L), "snapshots.contains(1792185872197984875L)");

        final Table table = adapter.listSnapshotsAsTable(tableIdentifier);
        Assert.eq(table.size(), "table.size()", 4, "4 snapshots for sales/sales_multi");
        Assert.eqTrue(table.getColumnSource("id").getType().equals(long.class), "id column type");
        Assert.eqTrue(table.getColumnSource("timestamp_ms").getType().equals(long.class), "timestamp_ms column type");
        Assert.eqTrue(table.getColumnSource("operation").getType().equals(String.class), "operation column type");
        Assert.eqTrue(table.getColumnSource("summary").getType().equals(Map.class), "summary column type");
        Assert.eqTrue(table.getColumnSource("snapshot_object").getType().equals(Snapshot.class),
                "snapshot_object column type");
    }

    @Test
    public void testOpenTableA() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, instructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableB() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_multi").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_multi");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, instructions);

        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableC() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_single").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_single");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, instructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableS3Only() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, instructions);

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
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);

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
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        try {
            final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);
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
                .dataInstructions(instructions.dataInstructions().get())
                .putColumnRenames("Region", "RegionName")
                .putColumnRenames("Item_Type", "ItemType")
                .putColumnRenames("Units_Sold", "UnitsSold")
                .putColumnRenames("Unit_Price", "UnitPrice")
                .putColumnRenames("Order_Date", "OrderDate")
                .putColumnRenames("year", "__year")
                .putColumnRenames("month", "__month")
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testSkippedPartitioningColumn() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("year").withPartitioning(),
                // Omitting month partitioning column
                ColumnDefinition.ofString("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.fromGenericType("Order_Date", Instant.class));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testReorderedPartitioningColumn() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("month").withPartitioning(),
                ColumnDefinition.ofInt("year").withPartitioning(),
                ColumnDefinition.ofString("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.fromGenericType("Order_Date", Instant.class));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testZeroPartitioningColumns() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofString("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.fromGenericType("Order_Date", Instant.class));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testIncorrectPartitioningColumns() throws ExecutionException, InterruptedException, TimeoutException {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("month").withPartitioning(),
                ColumnDefinition.ofInt("year").withPartitioning(),
                ColumnDefinition.ofString("Region").withPartitioning(),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.fromGenericType("Order_Date", Instant.class));

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .tableDefinition(tableDef)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");

        try {
            final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);
            Assert.statementNeverExecuted("Expected an exception for missing columns");
        } catch (final TableDataException e) {
            Assert.eqTrue(e.getMessage().startsWith("The following columns are not partitioned"), "Exception message");
        }
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
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        try {
            final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);
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
                .dataInstructions(instructions.dataInstructions().get())
                .putColumnRenames("RegionName", "Region")
                .putColumnRenames("ItemType", "Item_Type")
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableColumnRenamePartitioningColumns()
            throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);

        final IcebergInstructions localInstructions = IcebergInstructions.builder()
                .dataInstructions(instructions.dataInstructions().get())
                .putColumnRenames("VendorID", "vendor_id")
                .putColumnRenames("month", "__month")
                .putColumnRenames("year", "__year")
                .build();

        final IcebergCatalogAdapter adapter =
                IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_partitioned");
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableSnapshot() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_multi").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_multi");
        final List<Snapshot> snapshots = adapter.listSnapshots(tableId);

        // Verify we retrieved all the rows.
        final io.deephaven.engine.table.Table table0 =
                adapter.readTable(tableId, snapshots.get(0).snapshotId(), instructions);
        Assert.eq(table0.size(), "table0.size()", 18266, "18266 rows in the table");

        final io.deephaven.engine.table.Table table1 =
                adapter.readTable(tableId, snapshots.get(1).snapshotId(), instructions);
        Assert.eq(table1.size(), "table1.size()", 54373, "54373 rows in the table");

        final io.deephaven.engine.table.Table table2 =
                adapter.readTable(tableId, snapshots.get(2).snapshotId(), instructions);
        Assert.eq(table2.size(), "table2.size()", 72603, "72603 rows in the table");

        final io.deephaven.engine.table.Table table3 =
                adapter.readTable(tableId, snapshots.get(3).snapshotId(), instructions);
        Assert.eq(table3.size(), "table3.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenTableSnapshotByID() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_multi").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sales");
        final TableIdentifier tableId = TableIdentifier.of(ns, "sales_multi");
        final List<Snapshot> snapshots = adapter.listSnapshots(tableId);

        // Verify we retrieved all the rows.
        final io.deephaven.engine.table.Table table0 = adapter.readTable(tableId, snapshots.get(0), instructions);
        Assert.eq(table0.size(), "table0.size()", 18266, "18266 rows in the table");

        final io.deephaven.engine.table.Table table1 = adapter.readTable(tableId, snapshots.get(1), instructions);
        Assert.eq(table1.size(), "table1.size()", 54373, "54373 rows in the table");

        final io.deephaven.engine.table.Table table2 = adapter.readTable(tableId, snapshots.get(2), instructions);
        Assert.eq(table2.size(), "table2.size()", 72603, "72603 rows in the table");

        final io.deephaven.engine.table.Table table3 = adapter.readTable(tableId, snapshots.get(3), instructions);
        Assert.eq(table3.size(), "table3.size()", 100_000, "100_000 rows in the table");
    }

    @Test
    public void testOpenAllTypesTable() throws ExecutionException, InterruptedException, TimeoutException {
        uploadParquetFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sample/all_types").getPath()),
                warehousePath);

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog, resourceFileIO);

        final Namespace ns = Namespace.of("sample");
        final TableIdentifier tableId = TableIdentifier.of(ns, "all_types");
        final List<Snapshot> snapshots = adapter.listSnapshots(tableId);

        // Verify we retrieved all the rows.
        final io.deephaven.engine.table.Table table = adapter.readTable(tableId, instructions);
        Assert.eq(table.size(), "table.size()", 10, "10 rows in the table");
    }
}
