//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.File;
import java.net.URI;
import java.time.Duration;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.merge;
import static io.deephaven.parquet.table.ParquetTools.writeKeyValuePartitionedTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

abstract class S3ParquetTests extends S3SeekableChannelTestBase {

    private static final File rootDir = new File(S3ParquetTests.class.getName() + "_root");

    // The following tests are disabled by default, as they are verifying against a remote system
    private static final boolean ENABLE_REMOTE_S3_TESTING = false;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Before
    public void setUp() {
        super.setUp();
        if (rootDir.exists()) {
            FileUtils.deleteRecursively(rootDir);
        }
        // noinspection ResultOfMethodCallIgnored
        rootDir.mkdirs();
    }

    @After
    public void tearDown() {
        super.tearDown();
        FileUtils.deleteRecursively(rootDir);
    }

    @Test
    public final void readSingleParquetFile() {
        final Table table = TableTools.emptyTable(500_000).update(
                "someIntColumn = (int) i",
                "someDoubleColumn = (double) i",
                "someStringColumn = String.valueOf(i)",
                "someBooleanColumn = i % 2 == 0",
                "someCharColumn = (char) (i % 26 + 'a')");
        final File dest = new File(rootDir, "table.parquet");
        ParquetTools.writeTable(table, dest.getAbsolutePath());
        putObject("table.parquet", RequestBody.fromFile(dest));

        final URI uri = uri("table.parquet");
        final ParquetInstructions readInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .fragmentSize(65535)
                                .readAheadCount(8)
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .build();
        final Table fromS3 = ParquetTools.readTable(uri.toString(), readInstructions);
        assertTableEquals(table, fromS3);
    }

    @Test
    public final void readFlatPartitionedParquetData() {
        final Table table = TableTools.emptyTable(100_000).select(
                "someIntColumn = (int) i",
                "someDoubleColumn = (double) i",
                "someStringColumn = String.valueOf(i)",
                "someBooleanColumn = i % 2 == 0",
                "someCharColumn = (char) (i % 26 + 'a')");
        final String destDirName = "flatPartitionedDataDir";
        final File destDir = new File(rootDir, destDirName);
        for (int i = 0; i < 3; ++i) {
            final File dest = new File(destDir, "table" + i + ".parquet");
            ParquetTools.writeTable(table, dest.getAbsolutePath());
        }
        putDirectory(destDir);
        final URI uri = uri(destDirName);
        final ParquetInstructions readInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .fragmentSize(65535)
                                .readAheadCount(8)
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .build();

        final Table expected = merge(table, table, table);
        final Table fromS3AsFlat = ParquetTools.readTable(uri.toString(),
                readInstructions.withLayout(ParquetInstructions.ParquetFileLayout.FLAT_PARTITIONED));
        assertTableEquals(expected, fromS3AsFlat);

        final Table fromS3AsKV = ParquetTools.readTable(uri.toString(),
                readInstructions.withLayout(ParquetInstructions.ParquetFileLayout.KV_PARTITIONED));
        assertTableEquals(expected, fromS3AsKV);
    }

    @Test
    public void readKeyValuePartitionedParquetData() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("PC1").withPartitioning(),
                ColumnDefinition.ofInt("PC2").withPartitioning(),
                ColumnDefinition.ofInt("someIntColumn"),
                ColumnDefinition.ofString("someStringColumn"));
        final Table table = ((QueryTable) TableTools.emptyTable(500_000)
                .updateView("PC1 = (int)(ii%3)",
                        "PC2 = (int)(ii%2)",
                        "someIntColumn = (int) i",
                        "someStringColumn = String.valueOf(i)"))
                .withDefinitionUnsafe(definition);
        final String destDirName = "keyValuePartitionedDataDir";
        final File destDir = new File(rootDir, destDirName);
        final ParquetInstructions writeInstructions = ParquetInstructions.builder()
                .setBaseNameForPartitionedParquetData("data")
                .build();
        writeKeyValuePartitionedTable(table, destDir.getPath(), writeInstructions);
        putDirectory(destDir);
        final URI uri = uri(destDirName);
        final ParquetInstructions readInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .fragmentSize(65535)
                                .readAheadCount(8)
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .setTableDefinition(definition)
                .build();
        final Table fromS3 = ParquetTools.readTable(uri.toString(), readInstructions);
        assertTrue(fromS3.getDefinition().getColumn("PC1").isPartitioning());
        assertTrue(fromS3.getDefinition().getColumn("PC2").isPartitioning());
        assertTableEquals(table.sort("PC1", "PC2"), fromS3);
    }

    @Test
    public void readSampleParquetFilesFromPublicS3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_REMOTE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-2")
                .readAheadCount(1)
                .fragmentSize(5 * 1024 * 1024)
                .maxConcurrentRequests(50)
                .maxCacheSize(32)
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
    public void readKeyValuePartitionedParquetFromPublicS3() {
        Assume.assumeTrue("Skipping test because s3 testing disabled.", ENABLE_REMOTE_S3_TESTING);
        final S3Instructions s3Instructions = S3Instructions.builder()
                .regionName("us-east-1")
                .readAheadCount(1)
                .fragmentSize(5 * 1024 * 1024)
                .maxConcurrentRequests(50)
                .maxCacheSize(32)
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
