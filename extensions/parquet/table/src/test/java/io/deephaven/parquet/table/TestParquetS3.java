//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.test.types.OutOfBandTest;

import java.time.Duration;
import java.util.List;

import org.junit.*;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.emptyTable;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public final class TestParquetS3 {

    // TODO(deephaven-core#5064): Add support for local S3 testing
    // The following tests are disabled by default, as they are verifying against a remote system
    private static final boolean ENABLE_S3_TESTING =
            Configuration.getInstance().getBooleanWithDefault("ParquetTest.enableS3Testing", false);

    @Rule
    public final EngineCleanup framework = new EngineCleanup();


    @Test
    public void readSampleParquetFilesFromDeephavenS3Bucket() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-1")
                .readTimeout(Duration.ofSeconds(60))
                .credentials(Credentials.defaultCredentials())
                .build();
        final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                .setSpecialInstructions(s3Instructions)
                .build();
        final Table fromAws1 =
                ParquetTools.readTable("s3://dh-s3-parquet-test1/multiColFile.parquet", readInstructions).select();
        final Table dhTable1 = TableTools.emptyTable(1_000_000).update("A=(int)i", "B=(double)(i+1)");
        assertTableEquals(fromAws1, dhTable1);

        final Table fromAws2 =
                ParquetTools.readTable("s3://dh-s3-parquet-test1/singleColFile.parquet", readInstructions).select();
        final Table dhTable2 = TableTools.emptyTable(5).update("A=(int)i");
        assertTableEquals(fromAws2, dhTable2);

        final Table fromAws3 = ParquetTools
                .readTable("s3://dh-s3-parquet-test1/single%20col%20file%20with%20spaces%20in%20name.parquet",
                        readInstructions)
                .select();
        assertTableEquals(fromAws3, dhTable2);

        final Table fromAws4 =
                ParquetTools.readTable("s3://dh-s3-parquet-test1/singleColFile.parquet", readInstructions)
                        .select().sumBy();
        final Table dhTable4 = TableTools.emptyTable(5).update("A=(int)i").sumBy();
        assertTableEquals(fromAws4, dhTable4);
    }

    @Test
    public void readSampleParquetFilesFromPublicS3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-2")
                .connectionTimeout(Duration.ofSeconds(1))
                .readTimeout(Duration.ofSeconds(60))
                .credentials(Credentials.anonymous())
                .build();
        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofString("hash"),
                ColumnDefinition.ofLong("version"),
                ColumnDefinition.ofLong("size"),
                ColumnDefinition.ofString("block_hash"),
                ColumnDefinition.ofLong("block_number"),
                ColumnDefinition.ofLong("index"),
                ColumnDefinition.ofLong("virtual_size"),
                ColumnDefinition.ofLong("lock_time"),
                ColumnDefinition.ofLong("input_count"),
                ColumnDefinition.ofLong("output_count"),
                ColumnDefinition.ofBoolean("isCoinbase"),
                ColumnDefinition.ofDouble("output_value"),
                ColumnDefinition.ofTime("last_modified"),
                ColumnDefinition.ofDouble("input_value"));
        final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                .setSpecialInstructions(s3Instructions)
                .setTableDefinition(tableDefinition)
                .build();
        ParquetTools.readTable(
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2009-01-03/part-00000-bdd84ab2-82e9-4a79-8212-7accd76815e8-c000.snappy.parquet",
                readInstructions).head(10).select();

        ParquetTools.readTable(
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2023-11-13/part-00000-da3a3c27-700d-496d-9c41-81281388eca8-c000.snappy.parquet",
                readInstructions).head(10).select();
    }

    @Test
    public void readFlatPartitionedParquetFromS3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-1")
                .readTimeout(Duration.ofSeconds(60))
                .credentials(Credentials.defaultCredentials())
                .build();
        final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                .setSpecialInstructions(s3Instructions)
                .setFileLayout(ParquetInstructions.ParquetFileLayout.FLAT_PARTITIONED)
                .build();
        final Table table = ParquetTools.readTable("s3://dh-s3-parquet-test1/flatPartitionedParquet/",
                readInstructions);
        final Table expected = emptyTable(30).update("A = (int)i % 10");
        assertTableEquals(expected, table);
    }

    @Test
    public void readFlatPartitionedDataAsKeyValuePartitionedParquetFromS3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-1")
                .readTimeout(Duration.ofSeconds(60))
                .credentials(Credentials.defaultCredentials())
                .build();
        final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                .setSpecialInstructions(s3Instructions)
                .setFileLayout(ParquetInstructions.ParquetFileLayout.KV_PARTITIONED)
                .build();
        final Table table = ParquetTools.readTable("s3://dh-s3-parquet-test1/flatPartitionedParquet3/",
                readInstructions);
        final Table expected = emptyTable(30).update("A = (int)i % 10");
        assertTableEquals(expected, table);
    }

    @Test
    public void readKeyValuePartitionedParquetFromS3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-1")
                .readTimeout(Duration.ofSeconds(60))
                .credentials(Credentials.defaultCredentials())
                .build();
        final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                .setSpecialInstructions(s3Instructions)
                .setFileLayout(ParquetInstructions.ParquetFileLayout.KV_PARTITIONED)
                .build();
        final Table table = ParquetTools.readTable("s3://dh-s3-parquet-test1/KeyValuePartitionedData/",
                readInstructions);
        final List<ColumnDefinition<?>> partitioningColumns = table.getDefinition().getPartitioningColumns();
        assertEquals(3, partitioningColumns.size());
        assertEquals("PC1", partitioningColumns.get(0).getName());
        assertEquals("PC2", partitioningColumns.get(1).getName());
        assertEquals("PC3", partitioningColumns.get(2).getName());
        assertEquals(100, table.size());
        assertEquals(3, table.selectDistinct("PC1").size());
        assertEquals(2, table.selectDistinct("PC2").size());
        assertEquals(2, table.selectDistinct("PC3").size());
        assertEquals(100, table.selectDistinct("I").size());
        assertEquals(1, table.selectDistinct("J").size());
    }

    @Test
    public void readKeyValuePartitionedParquetFromPublicS3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-1")
                .readTimeout(Duration.ofSeconds(60))
                .credentials(Credentials.anonymous())
                .build();
        final TableDefinition ookla_table_definition = TableDefinition.of(
                ColumnDefinition.ofInt("quarter").withPartitioning(),
                ColumnDefinition.ofString("quadkey"));
        final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                .setSpecialInstructions(s3Instructions)
                .setTableDefinition(ookla_table_definition)
                .build();
        final Table table = ParquetTools.readTable("s3://ookla-open-data/parquet/performance/type=mobile/year=2023",
                readInstructions).head(10).select();
        assertEquals(2, table.numColumns());
    }
}
