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
import io.deephaven.extensions.s3.testlib.S3Helper;
import io.deephaven.iceberg.TestCatalog.IcebergTestCatalog;
import io.deephaven.iceberg.TestCatalog.IcebergTestFileIO;
import io.deephaven.iceberg.TestCatalog.IcebergTestSnapshot;
import io.deephaven.iceberg.TestCatalog.IcebergTestTable;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergInstructions;
import io.deephaven.iceberg.util.IcebergTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.time.DateTimeUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public abstract class IcebergToolsTest {
    IcebergInstructions instructions;
    IcebergInstructions instructionsS3Only;

    public abstract S3AsyncClient s3AsyncClient();

    public abstract S3Instructions.Builder s3Instructions(S3Instructions.Builder builder);

    private S3AsyncClient asyncClient;
    private String bucket;

    private final List<String> keys = new ArrayList<>();

    private IcebergTestCatalog catalog;
    private IcebergTestFileIO fileIO;

    @BeforeEach
    void setUp() {
        bucket = UUID.randomUUID().toString();
        asyncClient = s3AsyncClient();
        asyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

        fileIO = IcebergTestFileIO.INSTANCE;

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


        // Create a compatible schema
        // Create a snapshot
        //

        S3Helper.uploadDirectory();



        // Create the test catalog for the tests
        catalog = IcebergTestCatalog.create();

        // Create the schema for the tests
        final Schema taxisSchema = new Schema(0,
                Types.NestedField.optional(1, "VendorID", Types.LongType.get()),
                Types.NestedField.optional(2, "tpep_pickup_datetime", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(3, "tpep_dropoff_datetime", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(4, "passenger_count", Types.DoubleType.get()),
                Types.NestedField.optional(5, "trip_distance", Types.DoubleType.get()),
                Types.NestedField.optional(6, "RatecodeID", Types.DoubleType.get()),
                Types.NestedField.optional(7, "store_and_fwd_flag", Types.StringType.get()),
                Types.NestedField.optional(8, "PULocationID", Types.LongType.get()),
                Types.NestedField.optional(9, "DOLocationID", Types.LongType.get()),
                Types.NestedField.optional(10, "payment_type", Types.LongType.get()),
                Types.NestedField.optional(11, "fare_amount", Types.DoubleType.get()),
                Types.NestedField.optional(12, "extra", Types.DoubleType.get()),
                Types.NestedField.optional(13, "mta_tax", Types.DoubleType.get()),
                Types.NestedField.optional(14, "tip_amount", Types.DoubleType.get()),
                Types.NestedField.optional(15, "tolls_amount", Types.DoubleType.get()),
                Types.NestedField.optional(16, "improvement_surcharge", Types.DoubleType.get()),
                Types.NestedField.optional(17, "total_amount", Types.DoubleType.get()),
                Types.NestedField.optional(18, "congestion_surcharge", Types.DoubleType.get()),
                Types.NestedField.optional(19, "airport_fee", Types.DoubleType.get())
//                Types.NestedField.optional(20, "year", Types.IntegerType.get()),
//                Types.NestedField.optional(21, "month", Types.IntegerType.get())
        );

        final PartitionSpec taxisPartitionedSpec = PartitionSpec.builderFor(taxisSchema)
//                .identity("year")
//                .identity("month")
                .build();

        final IcebergTestSnapshot taxisSnapshot = IcebergTestSnapshot.create(0);
        // Improve this to use the actual data files in resources
        taxisSnapshot.addDataManifest(fileIO,
                "s3a://warehouse/wh/nyc/taxis_single/data/00002-4-825545b9-fc45-47ac-9f75-74ded525e6d6-00001.parquet");

        final IcebergTestTable table = IcebergTestTable.create(
                TableIdentifier.of(Namespace.of("nyc"), "taxis_single"),
                taxisSchema,
                taxisPartitionedSpec,
                taxisSnapshot);

        catalog.addTable(TableIdentifier.of("nyc", "taxis_single"), table);
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
    public void testOpenTableTestCatalog() {
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(catalog, fileIO, instructions);

        final Namespace ns = Namespace.of("nyc");
        final TableIdentifier tableId = TableIdentifier.of(ns, "taxis_single");
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
