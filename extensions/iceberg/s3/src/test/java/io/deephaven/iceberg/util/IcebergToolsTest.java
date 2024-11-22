//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.TestCatalog.IcebergTestCatalog;
import org.apache.iceberg.Schema;
import io.deephaven.iceberg.base.IcebergUtils;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.deephaven.iceberg.util.IcebergCatalogAdapter.NAMESPACE_DEFINITION;
import static io.deephaven.iceberg.util.IcebergCatalogAdapter.TABLES_DEFINITION;
import static io.deephaven.iceberg.util.IcebergTableAdapter.SNAPSHOT_DEFINITION;

/**
 * @deprecated tests against a fresh catalog should be added to {@link io.deephaven.iceberg.junit5.SqliteCatalogBase}
 *             and tests against pre-created catalogs should likely be migrated
 *             {@link io.deephaven.iceberg.sqlite.DbResource}
 */
@Deprecated
public abstract class IcebergToolsTest {

    private static final TableDefinition SALES_SINGLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("Region"),
            ColumnDefinition.ofString("Item_Type"),
            ColumnDefinition.ofInt("Units_Sold"),
            ColumnDefinition.ofDouble("Unit_Price"),
            ColumnDefinition.ofTime("Order_Date"));

    private static final TableDefinition SALES_RENAMED_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("Region_Name"),
            ColumnDefinition.ofString("ItemType"),
            ColumnDefinition.ofInt("UnitsSold"),
            ColumnDefinition.ofDouble("Unit_Price"),
            ColumnDefinition.ofTime("Order_Date"));

    private static final TableDefinition SALES_MULTI_DEFINITION = SALES_SINGLE_DEFINITION;

    private static final TableDefinition SALES_PARTITIONED_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("Region"),
            ColumnDefinition.ofString("Item_Type"),
            ColumnDefinition.ofInt("Units_Sold"),
            ColumnDefinition.ofDouble("Unit_Price"),
            ColumnDefinition.ofTime("Order_Date"),
            ColumnDefinition.ofInt("year").withPartitioning(),
            ColumnDefinition.ofInt("month").withPartitioning());

    private static final TableDefinition ALL_TYPES_DEF = TableDefinition.of(
            ColumnDefinition.ofBoolean("booleanField"),
            ColumnDefinition.ofInt("integerField"),
            ColumnDefinition.ofLong("longField"),
            ColumnDefinition.ofFloat("floatField"),
            ColumnDefinition.ofDouble("doubleField"),
            ColumnDefinition.ofString("stringField"),
            ColumnDefinition.fromGenericType("dateField", LocalDate.class),
            ColumnDefinition.fromGenericType("timeField", LocalTime.class),
            ColumnDefinition.fromGenericType("timestampField", LocalDateTime.class),
            ColumnDefinition.fromGenericType("decimalField", BigDecimal.class),
            ColumnDefinition.fromGenericType("fixedField", byte[].class),
            ColumnDefinition.fromGenericType("binaryField", byte[].class),
            ColumnDefinition.ofTime("instantField"));

    private static final TableDefinition META_DEF = TableDefinition.of(
            ColumnDefinition.ofString("Name"),
            ColumnDefinition.ofString("DataType"),
            ColumnDefinition.ofString("ColumnType"),
            ColumnDefinition.ofBoolean("IsPartitioning"));

    private IcebergReadInstructions instructions;

    public abstract S3AsyncClient s3AsyncClient();

    public abstract S3Instructions.Builder s3Instructions(S3Instructions.Builder builder);

    public abstract Map<String, String> properties();

    private S3AsyncClient asyncClient;
    private String bucket;

    private final List<String> keys = new ArrayList<>();

    private String warehousePath;
    private IcebergTestCatalog resourceCatalog;

    private final EngineCleanup framework = new EngineCleanup();

    @BeforeEach
    void setUp() throws Exception {
        framework.setUp();
        bucket = "warehouse";
        asyncClient = s3AsyncClient();
        asyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).get();

        warehousePath = IcebergToolsTest.class.getResource("/warehouse").getPath();

        // Create the test catalog for the tests
        resourceCatalog = IcebergTestCatalog.create(warehousePath, properties());

        final S3Instructions s3Instructions = s3Instructions(S3Instructions.builder()).build();

        instructions = IcebergReadInstructions.builder()
                .dataInstructions(s3Instructions)
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        resourceCatalog.close();
        for (String key : keys) {
            asyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build()).get();
        }
        keys.clear();
        asyncClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()).get();
        asyncClient.close();
        framework.tearDown();
    }

    private void uploadFiles(final File root, final String prefixToRemove)
            throws ExecutionException, InterruptedException, TimeoutException {
        for (final File file : root.listFiles()) {
            if (file.isDirectory()) {
                uploadFiles(file, prefixToRemove);
            } else {
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

    private void uploadSalesPartitioned() throws ExecutionException, InterruptedException, TimeoutException {
        uploadFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_partitioned").getPath()),
                warehousePath);
    }

    private void uploadAllTypes() throws ExecutionException, InterruptedException, TimeoutException {
        uploadFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sample/all_types").getPath()),
                warehousePath);
    }

    private void uploadSalesSingle() throws ExecutionException, InterruptedException, TimeoutException {
        uploadFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_single").getPath()),
                warehousePath);
    }

    private void uploadSalesMulti() throws ExecutionException, InterruptedException, TimeoutException {
        uploadFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_multi").getPath()),
                warehousePath);
    }

    private void uploadSalesRenamed() throws ExecutionException, InterruptedException, TimeoutException {
        uploadFiles(new File(IcebergToolsTest.class.getResource("/warehouse/sales/sales_renamed").getPath()),
                warehousePath);
    }

    @Test
    void testListNamespaces() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);

        final Collection<Namespace> namespaces = adapter.listNamespaces();
        final Collection<String> namespaceNames =
                namespaces.stream().map(Namespace::toString).collect(Collectors.toList());

        Assert.eq(namespaceNames.size(), "namespaceNames.size()", 2, "namespace in the catalog");
        Assert.eqTrue(namespaceNames.contains("sales"), "namespaceNames.contains(sales)");
        Assert.eqTrue(namespaceNames.contains("sample"), "namespaceNames.contains(sample)");

        final Table table = adapter.namespaces();
        Assert.eq(table.size(), "table.size()", 2, "namespace in the catalog");
        Assert.equals(table.getDefinition(), "table.getDefinition()", NAMESPACE_DEFINITION);
    }

    @Test
    void testListTables() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);

        final Namespace ns = Namespace.of("sales");

        Collection<TableIdentifier> tables = adapter.listTables(ns);
        Assert.eq(tables.size(), "tables.size()", 4, "tables in the namespace");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_multi")), "tables.contains(sales_multi)");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_partitioned")),
                "tables.contains(sales_partitioned)");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_single")), "tables.contains(sales_single)");
        Assert.eqTrue(tables.contains(TableIdentifier.of(ns, "sales_renamed")), "tables.contains(sales_renamed)");

        Table table = adapter.tables(ns);
        Assert.eq(table.size(), "table.size()", 4, "tables in the namespace");
        Assert.equals(table.getDefinition(), "table.getDefinition()", TABLES_DEFINITION);

        // Test the string versions of the methods
        table = adapter.tables("sales");
        Assert.eq(table.size(), "table.size()", 4, "tables in the namespace");
        Assert.equals(table.getDefinition(), "table.getDefinition()", TABLES_DEFINITION);
    }

    @Test
    void testGetTableAdapter() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);

        // Test the overloads of the load() method.
        final IcebergTableAdapter tableAdapter0 = adapter.loadTable("sales.sales_single");
        final IcebergTableAdapter tableAdapter1 = adapter.loadTable(TableIdentifier.of("sales", "sales_single"));

        Assert.eq(tableAdapter0.listSnapshots().size(), "tableAdapter0.listSnapshots().size()",
                tableAdapter1.listSnapshots().size(), "tableAdapter1.listSnapshots().size()");

        Assert.eq(tableAdapter0.currentSnapshot().timestampMillis(),
                "tableAdapter0.currentSnapshot().timestampMillis()",
                tableAdapter1.currentSnapshot().timestampMillis(), "tableAdapter1.currentSnapshot().timestampMillis()");
    }

    @Test
    void testListSnapshots() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_multi");

        final TLongArrayList snapshotIds = new TLongArrayList();

        tableAdapter.listSnapshots().forEach(snapshot -> snapshotIds.add(snapshot.snapshotId()));

        Assert.eq(snapshotIds.size(), "snapshots.size()", 6, "snapshots for sales/sales_multi");

        Assert.eqTrue(snapshotIds.contains(8121674823173822790L), "snapshots.contains(8121674823173822790L)");
        Assert.eqTrue(snapshotIds.contains(6040260770188877244L), "snapshots.contains(6040260770188877244L)");
        Assert.eqTrue(snapshotIds.contains(5693547373255393922L), "snapshots.contains(5693547373255393922L)");
        Assert.eqTrue(snapshotIds.contains(3445166522731196553L), "snapshots.contains(3445166522731196553L)");
        Assert.eqTrue(snapshotIds.contains(1277776933184906785L), "snapshots.contains(1277776933184906785L)");
        Assert.eqTrue(snapshotIds.contains(3825168261540020388L), "snapshots.contains(3825168261540020388L)");

        Table table = tableAdapter.snapshots();
        Assert.eq(table.size(), "table.size()", 6, "snapshots for sales/sales_multi");
        Assert.equals(table.getDefinition(), "table.getDefinition()", SNAPSHOT_DEFINITION);
    }

    @Test
    void testOpenTableA() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesPartitioned();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");
        final io.deephaven.engine.table.Table table = tableAdapter.table(instructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", SALES_PARTITIONED_DEFINITION);
    }

    @Test
    public void testOpenTableB() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesMulti();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_multi");
        final io.deephaven.engine.table.Table table = tableAdapter.table(instructions);

        // This table ends up with zero records
        Assert.eq(table.size(), "table.size()", 0, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", SALES_MULTI_DEFINITION);
    }

    @Test
    void testOpenTableC() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesSingle();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_single");
        final io.deephaven.engine.table.Table table = tableAdapter.table(instructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", SALES_SINGLE_DEFINITION);
    }

    @Test
    void testOpenTableS3Only() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesPartitioned();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");
        final io.deephaven.engine.table.Table table = tableAdapter.table(instructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", SALES_PARTITIONED_DEFINITION);
    }

    @Test
    void testOpenTableDefinition() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesPartitioned();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");
        final io.deephaven.engine.table.Table table = tableAdapter.table(instructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", SALES_PARTITIONED_DEFINITION);
    }

    @Test
    void testOpenTablePartitionTypeException() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofLong("year").withPartitioning(),
                ColumnDefinition.ofInt("month").withPartitioning(),
                ColumnDefinition.ofLong("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofDouble("Units_Sold"),
                ColumnDefinition.ofLong("Unit_Price"),
                ColumnDefinition.ofTime("Order_Date"));

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .tableDefinition(tableDef)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");

        for (Runnable runnable : Arrays.<Runnable>asList(
                () -> tableAdapter.table(localInstructions),
                () -> tableAdapter.definition(localInstructions),
                () -> tableAdapter.definitionTable(localInstructions))) {
            try {
                runnable.run();
                Assert.statementNeverExecuted("Expected an exception for missing columns");
            } catch (final TableDefinition.IncompatibleTableDefinitionException e) {
                Assert.eqTrue(e.getMessage().startsWith("Table definition incompatibilities"), "Exception message");
            }
        }
    }

    @Test
    void testOpenTableDefinitionRename() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesPartitioned();

        final TableDefinition renamed = TableDefinition.of(
                ColumnDefinition.ofInt("__year").withPartitioning(),
                ColumnDefinition.ofInt("__month").withPartitioning(),
                ColumnDefinition.ofString("RegionName"),
                ColumnDefinition.ofString("ItemType"),
                ColumnDefinition.ofInt("UnitsSold"),
                ColumnDefinition.ofDouble("UnitPrice"),
                ColumnDefinition.ofTime("OrderDate"));

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .tableDefinition(renamed)
                .dataInstructions(instructions.dataInstructions().get())
                .putColumnRenames("Region", "RegionName")
                .putColumnRenames("Item_Type", "ItemType")
                .putColumnRenames("Units_Sold", "UnitsSold")
                .putColumnRenames("Unit_Price", "UnitPrice")
                .putColumnRenames("Order_Date", "OrderDate")
                .putColumnRenames("year", "__year")
                .putColumnRenames("month", "__month")
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");
        final io.deephaven.engine.table.Table table = tableAdapter.table(localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", renamed);
    }

    @Test
    void testSkippedPartitioningColumn() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesPartitioned();

        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("year").withPartitioning(),
                // Omitting month partitioning column
                ColumnDefinition.ofString("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.ofTime("Order_Date"));

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .tableDefinition(tableDef)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");
        final io.deephaven.engine.table.Table table = tableAdapter.table(localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", tableDef);
    }


    @Test
    void testReorderedPartitioningColumn() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesPartitioned();

        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("month").withPartitioning(),
                ColumnDefinition.ofInt("year").withPartitioning(),
                ColumnDefinition.ofString("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.ofTime("Order_Date"));

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .tableDefinition(tableDef)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");
        final io.deephaven.engine.table.Table table = tableAdapter.table(localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", tableDef);
    }

    @Test
    void testZeroPartitioningColumns() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesPartitioned();

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .tableDefinition(SALES_MULTI_DEFINITION)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");
        final io.deephaven.engine.table.Table table = tableAdapter.table(localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", SALES_MULTI_DEFINITION);
    }

    @Test
    void testIncorrectPartitioningColumns() throws ExecutionException, InterruptedException, TimeoutException {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("month").withPartitioning(),
                ColumnDefinition.ofInt("year").withPartitioning(),
                ColumnDefinition.ofString("Region").withPartitioning(),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.ofTime("Order_Date"));

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .tableDefinition(tableDef)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");

        for (Runnable runnable : Arrays.<Runnable>asList(
                () -> tableAdapter.table(localInstructions),
                () -> tableAdapter.definition(localInstructions),
                () -> tableAdapter.definitionTable(localInstructions))) {
            try {
                runnable.run();
                Assert.statementNeverExecuted("Expected an exception for missing columns");
            } catch (final TableDataException e) {
                Assert.eqTrue(e.getMessage().startsWith("The following columns are not partitioned"),
                        "Exception message");
            }
        }
    }

    @Test
    void testMissingPartitioningColumns() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("__year").withPartitioning(), // Incorrect name
                ColumnDefinition.ofInt("__month").withPartitioning(), // Incorrect name
                ColumnDefinition.ofLong("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofDouble("Units_Sold"),
                ColumnDefinition.ofLong("Unit_Price"),
                ColumnDefinition.ofTime("Order_Date"));

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .tableDefinition(tableDef)
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");

        for (Runnable runnable : Arrays.<Runnable>asList(
                () -> tableAdapter.table(localInstructions),
                () -> tableAdapter.definition(localInstructions),
                () -> tableAdapter.definitionTable(localInstructions))) {
            try {
                runnable.run();
                Assert.statementNeverExecuted("Expected an exception for missing columns");
            } catch (final TableDefinition.IncompatibleTableDefinitionException e) {
                Assert.eqTrue(e.getMessage().startsWith("Table definition incompatibilities"), "Exception message");
            }
        }
    }

    @Test
    void testOpenTableColumnRename() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesPartitioned();

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .dataInstructions(instructions.dataInstructions().get())
                .putColumnRenames("Region", "RegionName")
                .putColumnRenames("Item_Type", "ItemType")
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");
        final io.deephaven.engine.table.Table table = tableAdapter.table(localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
    }

    @Test
    void testOpenTableColumnLegalization() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesRenamed();

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_renamed");
        final io.deephaven.engine.table.Table table = tableAdapter.table(localInstructions);

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", SALES_RENAMED_DEFINITION);
    }

    @Test
    void testOpenTableColumnLegalizationRename()
            throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesRenamed();

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .dataInstructions(instructions.dataInstructions().get())
                .putColumnRenames("Item&Type", "Item_Type")
                .putColumnRenames("Units/Sold", "Units_Sold")
                .build();


        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_renamed");
        final io.deephaven.engine.table.Table table = tableAdapter.table(localInstructions);

        final TableDefinition expected = TableDefinition.of(
                ColumnDefinition.ofString("Region_Name"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.ofTime("Order_Date"));

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", expected);
    }

    @Test
    void testOpenTableColumnLegalizationPartitionException() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofInt("Year").withPartitioning(),
                ColumnDefinition.ofInt("Month").withPartitioning());

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .tableDefinition(tableDef)
                .putColumnRenames("Year", "Current Year")
                .putColumnRenames("Month", "Current Month")
                .dataInstructions(instructions.dataInstructions().get())
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");

        for (Runnable runnable : Arrays.<Runnable>asList(
                () -> tableAdapter.table(localInstructions),
                () -> tableAdapter.definition(localInstructions),
                () -> tableAdapter.definitionTable(localInstructions))) {
            try {
                runnable.run();
                Assert.statementNeverExecuted("Expected an exception for missing columns");
            } catch (final TableDataException e) {
                Assert.eqTrue(e.getMessage().contains("invalid column name provided"), "Exception message");
            }
        }
    }

    @Test
    void testOpenTableColumnRenamePartitioningColumns()
            throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesPartitioned();

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .dataInstructions(instructions.dataInstructions().get())
                .putColumnRenames("VendorID", "vendor_id")
                .putColumnRenames("month", "__month")
                .putColumnRenames("year", "__year")
                .build();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_partitioned");
        final io.deephaven.engine.table.Table table = tableAdapter.table(localInstructions);

        final TableDefinition expected = TableDefinition.of(
                ColumnDefinition.ofString("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofInt("Units_Sold"),
                ColumnDefinition.ofDouble("Unit_Price"),
                ColumnDefinition.ofTime("Order_Date"),
                ColumnDefinition.ofInt("__year").withPartitioning(),
                ColumnDefinition.ofInt("__month").withPartitioning());

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 100_000, "100_000 rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", expected);
    }

    @Test
    void testOpenTableSnapshot() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesMulti();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_multi");
        final List<Snapshot> snapshots = tableAdapter.listSnapshots();

        // Verify we retrieved all the rows.
        final io.deephaven.engine.table.Table table0 =
                tableAdapter.table(instructions.withSnapshotId(snapshots.get(0).snapshotId()));
        Assert.eq(table0.size(), "table0.size()", 18073, "expected rows in the table");
        Assert.equals(table0.getDefinition(), "table0.getDefinition()", SALES_MULTI_DEFINITION);

        final io.deephaven.engine.table.Table table1 =
                tableAdapter.table(instructions.withSnapshotId(snapshots.get(1).snapshotId()));
        Assert.eq(table1.size(), "table1.size()", 54433, "expected rows in the table");
        Assert.equals(table1.getDefinition(), "table1.getDefinition()", SALES_MULTI_DEFINITION);

        final io.deephaven.engine.table.Table table2 =
                tableAdapter.table(instructions.withSnapshotId(snapshots.get(2).snapshotId()));
        Assert.eq(table2.size(), "table2.size()", 72551, "expected rows in the table");
        Assert.equals(table2.getDefinition(), "table2.getDefinition()", SALES_MULTI_DEFINITION);

        final io.deephaven.engine.table.Table table3 =
                tableAdapter.table(instructions.withSnapshotId(snapshots.get(3).snapshotId()));
        Assert.eq(table3.size(), "table3.size()", 100_000, "expected rows in the table");
        Assert.equals(table3.getDefinition(), "table3.getDefinition()", SALES_MULTI_DEFINITION);

        final io.deephaven.engine.table.Table table4 =
                tableAdapter.table(instructions.withSnapshotId(snapshots.get(4).snapshotId()));
        Assert.eq(table4.size(), "table4.size()", 100_000, "expected rows in the table");
        Assert.equals(table4.getDefinition(), "table4.getDefinition()", SALES_MULTI_DEFINITION);

        final io.deephaven.engine.table.Table table5 =
                tableAdapter.table(instructions.withSnapshotId(snapshots.get(5).snapshotId()));
        Assert.eq(table5.size(), "table5.size()", 0, "expected rows in the table");
        Assert.equals(table5.getDefinition(), "table5.getDefinition()", SALES_MULTI_DEFINITION);
    }

    @Test
    void testOpenTableSnapshotByID() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesMulti();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_multi");
        final List<Snapshot> snapshots = tableAdapter.listSnapshots();

        // Verify we retrieved all the rows.
        io.deephaven.engine.table.Table table0 = tableAdapter.table(instructions.withSnapshot(snapshots.get(0)));
        Assert.eq(table0.size(), "table0.size()", 18073, "expected rows in the table");
        Assert.equals(table0.getDefinition(), "table0.getDefinition()", SALES_MULTI_DEFINITION);

        io.deephaven.engine.table.Table table1 = tableAdapter.table(instructions.withSnapshot(snapshots.get(1)));
        Assert.eq(table1.size(), "table1.size()", 54433, "expected rows in the table");
        Assert.equals(table1.getDefinition(), "table1.getDefinition()", SALES_MULTI_DEFINITION);

        io.deephaven.engine.table.Table table2 = tableAdapter.table(instructions.withSnapshot(snapshots.get(2)));
        Assert.eq(table2.size(), "table2.size()", 72551, "expected rows in the table");
        Assert.equals(table2.getDefinition(), "table2.getDefinition()", SALES_MULTI_DEFINITION);

        io.deephaven.engine.table.Table table3 = tableAdapter.table(instructions.withSnapshot(snapshots.get(3)));
        Assert.eq(table3.size(), "table3.size()", 100_000, "expected rows in the table");
        Assert.equals(table3.getDefinition(), "table3.getDefinition()", SALES_MULTI_DEFINITION);

        io.deephaven.engine.table.Table table4 = tableAdapter.table(instructions.withSnapshot(snapshots.get(4)));
        Assert.eq(table4.size(), "table4.size()", 100_000, "expected rows in the table");
        Assert.equals(table4.getDefinition(), "table4.getDefinition()", SALES_MULTI_DEFINITION);

        io.deephaven.engine.table.Table table5 = tableAdapter.table(instructions.withSnapshot(snapshots.get(5)));
        Assert.eq(table5.size(), "table5.size()", 0, "expected rows in the table");
        Assert.equals(table5.getDefinition(), "table5.getDefinition()", SALES_MULTI_DEFINITION);

        try {
            io.deephaven.engine.table.Table missing = tableAdapter.table(instructions.withSnapshotId(987654321L));
            Assert.statementNeverExecuted("Expected an exception for invalid snapshot");
        } catch (final Exception e) {
            Assert.assertion(e instanceof IllegalArgumentException, "e instanceof IllegalArgumentException");
            Assert.eqTrue(e.getMessage().contains("Snapshot with id 987654321 not found"), "Exception message");
        }
    }

    @Test
    void testOpenAllTypesTable() throws ExecutionException, InterruptedException, TimeoutException {
        uploadAllTypes();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sample.all_types");
        final io.deephaven.engine.table.Table table = tableAdapter.table(instructions).select();

        // Verify we retrieved all the rows.
        Assert.eq(table.size(), "table.size()", 10, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", ALL_TYPES_DEF);
    }

    @Test
    void testTableDefinition() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_multi");
        final List<Snapshot> snapshots = tableAdapter.listSnapshots();

        // Use string and current snapshot
        TableDefinition tableDef = tableAdapter.definition();
        Assert.equals(tableDef, "tableDef", SALES_MULTI_DEFINITION);

        // Use TableIdentifier and Snapshot
        tableDef = tableAdapter.definition(instructions);
        Assert.equals(tableDef, "tableDef", SALES_MULTI_DEFINITION);

        // Use string and long snapshot ID
        tableDef = tableAdapter.definition(IcebergReadInstructions.builder()
                .snapshotId(snapshots.get(0).snapshotId())
                .build());
        Assert.equals(tableDef, "tableDef", SALES_MULTI_DEFINITION);

        // Use TableIdentifier and Snapshot
        tableDef = tableAdapter.definition(IcebergReadInstructions.builder()
                .snapshot(snapshots.get(0))
                .build());
        Assert.equals(tableDef, "tableDef", SALES_MULTI_DEFINITION);
    }

    @Test
    void testTableSchema() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_multi");

        // Request a schema that does not exist.
        final Optional<Schema> missingSchema = tableAdapter.schema(1000);
        Assert.eqFalse(missingSchema.isPresent(), "schema.isPresent()");

        // Request a schema that does exist.
        final Optional<Schema> schema0 = tableAdapter.schema(0);
        Assert.eqTrue(schema0.isPresent(), "schema.isPresent()");

        // Request the current schema, assert it matches schema0
        final Schema currentSchema = tableAdapter.currentSchema();
        Assert.eq(currentSchema, "currentSchema", schema0.get(), "schema0.get()");

        // Request the schema map.
        final Map<Integer, Schema> schemaMap = tableAdapter.schemas();
        Assert.eq(schemaMap.size(), "schemaMap.size()", 1, "expected number of schemas");
    }

    @Test
    void testTableDefinitionTable() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_multi");
        final List<Snapshot> snapshots = tableAdapter.listSnapshots();

        // Use string and current snapshot
        Table tableDefTable = tableAdapter.definitionTable();

        Assert.eq(tableDefTable.size(), "tableDefTable.size()", 5, "expected rows in the table");
        Assert.equals(tableDefTable.getDefinition(), "tableDefTable.getDefinition()", META_DEF);

        // Use TableIdentifier and Snapshot
        tableDefTable = tableAdapter.definitionTable(instructions);

        Assert.eq(tableDefTable.size(), "tableDefTable.size()", 5, "expected rows in the table");
        Assert.equals(tableDefTable.getDefinition(), "tableDefTable.getDefinition()", META_DEF);

        // Use string and long snapshot ID
        tableDefTable = tableAdapter
                .definitionTable(IcebergReadInstructions.DEFAULT.withSnapshotId(snapshots.get(0).snapshotId()));

        Assert.eq(tableDefTable.size(), "tableDefTable.size()", 5, "expected rows in the table");
        Assert.equals(tableDefTable.getDefinition(), "tableDefTable.getDefinition()", META_DEF);

        // Use TableIdentifier and Snapshot
        tableDefTable = tableAdapter.definitionTable(IcebergReadInstructions.DEFAULT.withSnapshot(snapshots.get(0)));

        Assert.eq(tableDefTable.size(), "tableDefTable.size()", 5, "expected rows in the table");
        Assert.equals(tableDefTable.getDefinition(), "tableDefTable.getDefinition()", META_DEF);
    }

    @Test
    void testTableDefinitionWithInstructions() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);
        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_multi");

        IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .dataInstructions(instructions.dataInstructions().get())
                .putColumnRenames("Region", "Area")
                .putColumnRenames("Item_Type", "ItemType")
                .putColumnRenames("Units_Sold", "UnitsSold")
                .putColumnRenames("Unit_Price", "UnitPrice")
                .putColumnRenames("Order_Date", "OrderDate")
                .build();

        final TableDefinition renamed = TableDefinition.of(
                ColumnDefinition.ofString("Area"),
                ColumnDefinition.ofString("ItemType"),
                ColumnDefinition.ofInt("UnitsSold"),
                ColumnDefinition.ofDouble("UnitPrice"),
                ColumnDefinition.ofTime("OrderDate"));

        // Use string and current snapshot
        TableDefinition tableDef = tableAdapter.definition(localInstructions);
        Assert.equals(tableDef, "tableDef", renamed);

        /////////////////////////////////////////////////////

        final TableDefinition userTableDef = TableDefinition.of(
                ColumnDefinition.ofString("Region"),
                ColumnDefinition.ofString("Item_Type"),
                ColumnDefinition.ofTime("Order_Date"));

        localInstructions = IcebergReadInstructions.builder()
                .dataInstructions(instructions.dataInstructions().get())
                .tableDefinition(userTableDef)
                .build();

        // Use string and current snapshot
        tableDef = tableAdapter.definition(localInstructions);
        Assert.equals(tableDef, "tableDef", userTableDef);
    }

    @Test
    void testManualRefreshingTable() throws ExecutionException, InterruptedException, TimeoutException {
        uploadSalesMulti();

        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(resourceCatalog);

        final IcebergReadInstructions localInstructions = IcebergReadInstructions.builder()
                .dataInstructions(instructions.dataInstructions().get())
                .updateMode(IcebergUpdateMode.manualRefreshingMode())
                .build();

        final IcebergTableAdapter tableAdapter = adapter.loadTable("sales.sales_multi");
        final List<Snapshot> snapshots = tableAdapter.listSnapshots();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final IcebergTableImpl table =
                (IcebergTableImpl) tableAdapter.table(localInstructions.withSnapshot(snapshots.get(0)));

        // Initial size
        Assert.eq(table.size(), "table.size()", 18073, "expected rows in the table");
        Assert.equals(table.getDefinition(), "table.getDefinition()", SALES_MULTI_DEFINITION);

        table.update(snapshots.get(1).snapshotId());
        updateGraph.runWithinUnitTestCycle(table::refresh);
        Assert.eq(table.size(), "table.size()", 54433, "expected rows in the table");

        table.update(snapshots.get(2).snapshotId());
        updateGraph.runWithinUnitTestCycle(table::refresh);
        Assert.eq(table.size(), "table.size()", 72551, "expected rows in the table");

        table.update(snapshots.get(3).snapshotId());
        updateGraph.runWithinUnitTestCycle(table::refresh);
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");

        table.update(snapshots.get(4).snapshotId());
        updateGraph.runWithinUnitTestCycle(table::refresh);
        Assert.eq(table.size(), "table.size()", 100_000, "expected rows in the table");

        table.update(snapshots.get(5).snapshotId());
        updateGraph.runWithinUnitTestCycle(table::refresh);
        Assert.eq(table.size(), "table.size()", 0, "expected rows in the table");
    }

    @Test
    void testConvertToIcebergTypeAndBack() {
        final Class<?>[] javaTypes = {
                Boolean.class, double.class, float.class, int.class, long.class, String.class, Instant.class,
                LocalDateTime.class, LocalDate.class, LocalTime.class, byte[].class
        };

        for (final Class<?> javaType : javaTypes) {
            // Java type -> Iceberg type
            final Type icebergType = IcebergUtils.convertToIcebergType(javaType);

            // Iceberg type -> Deephaven type
            final io.deephaven.qst.type.Type<?> deephavenType = IcebergUtils.convertToDHType(icebergType);

            // Deephaven type == Java type
            Assert.eq(javaType, javaType.getName(), deephavenType.clazz(), deephavenType.clazz().getName());
        }
    }
}
