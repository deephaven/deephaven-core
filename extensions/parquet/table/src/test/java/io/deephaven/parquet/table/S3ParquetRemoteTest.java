//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.parquet.table.ParquetTools.readTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * These tests verify the behavior of Parquet implementation when reading against remote S3 servers.
 **/
@Category(OutOfBandTest.class)
public class S3ParquetRemoteTest {

    // The following tests are disabled by default, and should be run manually.
    private static final boolean ENABLE_REMOTE_S3_TESTING = false;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void readSampleParquetFilesFromPublicS3Part1() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_REMOTE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-2")
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
    public void readSampleParquetFilesFromPublicS3Part2() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_REMOTE_S3_TESTING);
        {
            final S3Instructions s3Instructions = S3Instructions.builder()
                    .regionName("eu-west-3")
                    .readTimeout(Duration.ofSeconds(60))
                    .credentials(Credentials.anonymous())
                    .build();
            final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                    .setSpecialInstructions(s3Instructions)
                    .build();
            readTable("s3://datasets-documentation/pypi/2023/pypi_66_7_29.snappy.parquet", readInstructions)
                    .head(10).select();
        }

        // Now read the same file without a region
        {
            final S3Instructions s3Instructions = S3Instructions.builder()
                    .readTimeout(Duration.ofSeconds(60))
                    .credentials(Credentials.anonymous())
                    .build();
            final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                    .setSpecialInstructions(s3Instructions)
                    .build();
            readTable("s3://datasets-documentation/pypi/2023/pypi_66_7_29.snappy.parquet", readInstructions)
                    .head(10).select();
        }

        // Now read the same file with credentials not set as anonymous
        {
            final S3Instructions s3Instructions = S3Instructions.builder()
                    .readTimeout(Duration.ofSeconds(60))
                    .build();
            final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                    .setSpecialInstructions(s3Instructions)
                    .build();
            readTable("s3://datasets-documentation/pypi/2023/pypi_66_7_29.snappy.parquet", readInstructions)
                    .head(10).select();
        }
    }

    @Test
    public void readSampleParquetFilesFromPublicS3Part3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_REMOTE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-1")
                .readTimeout(Duration.ofSeconds(60))
                .credentials(Credentials.anonymous())
                .build();
        final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                .setSpecialInstructions(s3Instructions)
                .build();
        readTable("s3://redshift-downloads/redset/serverless/full.parquet", readInstructions).head(10).select();
    }

    @Test
    public void readSampleParquetFromPublicGCS() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_REMOTE_S3_TESTING);
        final Table tableWithEndpointOverride;
        {
            final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                    .setSpecialInstructions(S3Instructions.builder()
                            .readTimeout(Duration.ofSeconds(60))
                            .credentials(Credentials.anonymous())
                            .regionName("us-east-1")
                            .endpointOverride("https://storage.googleapis.com")
                            .build())
                    .build();
            tableWithEndpointOverride = ParquetTools.readTable(
                    "s3://cloud-samples-data/bigquery/us-states/us-states.parquet", readInstructions).select();
            assertEquals(2, tableWithEndpointOverride.numColumns());
            assertEquals(50, tableWithEndpointOverride.size());
        }

        final Table tableWithoutEndpointOverride;
        {
            final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                    .setSpecialInstructions(S3Instructions.builder()
                            .readTimeout(Duration.ofSeconds(60))
                            .regionName("us-east-1")
                            .credentials(Credentials.anonymous())
                            .build())
                    .build();
            tableWithoutEndpointOverride = ParquetTools.readTable(
                    "gs://cloud-samples-data/bigquery/us-states/us-states.parquet", readInstructions).select();
            assertEquals(2, tableWithoutEndpointOverride.numColumns());
            assertEquals(50, tableWithoutEndpointOverride.size());
        }
        assertTableEquals(tableWithEndpointOverride, tableWithoutEndpointOverride);

        final Table tableWithNoRegionAndCredentials;
        {
            // Note that this assumes that credentials are not present in the credentials file. If they are, this test
            // will fail.
            final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                    .setSpecialInstructions(S3Instructions.builder()
                            .readTimeout(Duration.ofSeconds(60))
                            .endpointOverride("https://storage.googleapis.com")
                            .build())
                    .build();
            tableWithNoRegionAndCredentials = ParquetTools.readTable(
                    "gs://cloud-samples-data/bigquery/us-states/us-states.parquet", readInstructions).select();
            assertEquals(2, tableWithNoRegionAndCredentials.numColumns());
            assertEquals(50, tableWithNoRegionAndCredentials.size());
        }
        assertTableEquals(tableWithEndpointOverride, tableWithNoRegionAndCredentials);
    }

    @Test
    public void testReadFromGCSFailure() {
        final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                .setSpecialInstructions(S3Instructions.builder()
                        .readTimeout(Duration.ofSeconds(60))
                        .credentials(Credentials.anonymous())
                        .endpointOverride("https://storage.com")
                        .build())
                .build();
        try {
            ParquetTools.readTable(
                    "gs://cloud-samples-data/bigquery/us-states/us-states.parquet", readInstructions).select();
        } catch (final IllegalArgumentException e) {
            assertTrue(e.toString().contains("endpoint override"));
        }
    }

    @Test
    public void readKeyValuePartitionedParquetFromPublicS3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_REMOTE_S3_TESTING);
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

    /**
     * The follow test reads from Deephaven's s3 bucket, thus requires the credentials to be set up, else will fail.
     */
    @Test
    public void readMetadataPartitionedParquetFromS3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_REMOTE_S3_TESTING);
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("PC1").withPartitioning(),
                ColumnDefinition.ofInt("PC2").withPartitioning(),
                ColumnDefinition.ofLong("I"));
        final Table source = ((QueryTable) TableTools.emptyTable(1_000_000)
                .updateView("PC1 = (int)(ii%3)",
                        "PC2 = (int)(ii%2)",
                        "I = ii"))
                .withDefinitionUnsafe(definition);

        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-1")
                .build();
        final ParquetInstructions readInstructions = new ParquetInstructions.Builder()
                .setSpecialInstructions(s3Instructions)
                .setFileLayout(ParquetInstructions.ParquetFileLayout.METADATA_PARTITIONED)
                .build();
        final Table fromS3Partitioned = readTable("s3://dh-s3-parquet-test1/keyValuePartitionedWithMetadataTest/",
                readInstructions);
        assertTableEquals(source.sort("PC1", "PC2"), fromS3Partitioned.sort("PC1", "PC2"));

        final Table fromDiskWithMetadata =
                readTable("s3://dh-s3-parquet-test1/keyValuePartitionedWithMetadataTest/_metadata",
                        readInstructions);
        assertTableEquals(source.sort("PC1", "PC2"), fromDiskWithMetadata.sort("PC1", "PC2"));

        final Table fromDiskWithCommonMetadata =
                readTable("s3://dh-s3-parquet-test1/keyValuePartitionedWithMetadataTest/_common_metadata",
                        readInstructions);
        assertTableEquals(source.sort("PC1", "PC2"), fromDiskWithCommonMetadata.sort("PC1", "PC2"));
    }
}
