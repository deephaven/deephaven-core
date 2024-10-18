//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.select.FormulaEvaluationException;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.extensions.s3.testlib.S3SeekableChannelTestSetup;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.core.async.AsyncRequestBody;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.merge;
import static io.deephaven.parquet.table.ParquetTableReadWriteTest.verifyIndexingInfoExists;
import static io.deephaven.parquet.table.ParquetTools.writeKeyValuePartitionedTable;
import static io.deephaven.parquet.table.ParquetTools.writeTable;
import static io.deephaven.parquet.table.ParquetTools.writeTables;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(OutOfBandTest.class)
abstract class S3ParquetTestBase extends S3SeekableChannelTestSetup {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    public abstract String s3Endpoint();

    public abstract String region();

    public abstract String accessKey();

    public abstract String secretAccessKey();

    @Before
    public void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        super.doSetUp();
    }

    @After
    public void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
        super.doTearDown();
    }

    private static Table getTable(final int numRows) {
        return TableTools.emptyTable(numRows).update(
                "someIntColumn = (int) i",
                "someDoubleColumn = (double) i",
                "someStringColumn = String.valueOf(i)",
                "someBooleanColumn = i % 2 == 0",
                "someCharColumn = (char) (i % 26 + 'a')");
    }

    @Test
    public final void readSingleParquetFile()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        final Table table = getTable(500_000);
        final URI uri = uri("table.parquet");
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .build();

        // Write the table to S3 using the test async client
        final File dest = new File(folder.newFolder(), "table.parquet");
        ParquetTools.writeTable(table, dest.getAbsolutePath());
        putObject("table.parquet", AsyncRequestBody.fromFile(dest));
        final Table fromS3 = ParquetTools.readTable(uri.toString(), instructions);
        assertTableEquals(table, fromS3);
    }

    @Test
    public final void readWriteSingleParquetFile() {
        readWriteSingleParquetFileHelper(0, true); // Empty table
        readWriteSingleParquetFileHelper(0, false);
        readWriteSingleParquetFileHelper(5_000, true);
        readWriteSingleParquetFileHelper(5_000, false);
        readWriteSingleParquetFileHelper(50_000, true);
        readWriteSingleParquetFileHelper(500_000, true);
    }

    private void readWriteSingleParquetFileHelper(final int numRows, boolean withRegion) {
        final Table table = getTable(numRows);
        final URI uri = uri("table.parquet");
        S3Instructions s3Instructions = s3Instructions(
                S3Instructions.builder()
                        .writePartSize(5 << 20)
                        .numConcurrentWriteParts(5)
                        .readTimeout(Duration.ofSeconds(10)))
                .build();
        if (!withRegion) {
            s3Instructions = s3Instructions.withRegionName(Optional.empty());
        }
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions)
                .build();

        // Write the table to S3 using ParquetTools write API
        ParquetTools.writeTable(table, uri.toString(), instructions);
        final Table fromS3 = ParquetTools.readTable(uri.toString(), instructions);
        assertTableEquals(table, fromS3);
    }

    @Test
    public final void mixURIWritingTest() {
        final Table table1, table2;
        table1 = table2 = getTable(5000);
        final String uri1 = uri("table1.parquet").toString();
        final String uri2 = new File(folder.getRoot(), "table2.parquet").toURI().toString();
        try {
            ParquetTools.writeTables(new Table[] {table1, table2}, new String[] {uri1, uri2},
                    ParquetInstructions.EMPTY);
            fail("Expected exception because writing to file and to S3 are not allowed in the same call");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("URIs must have the same scheme"));
        }
    }

    @Test
    public final void writeSingleTableExceptionTest() {
        final Table tableToSave = TableTools.emptyTable(5).update("A=(int)i", "B=(long)i", "C=(double)i");
        final URI fileUri = uri("destDir/table.parquet");
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .build();
        ParquetTools.writeTable(tableToSave, fileUri.toString(), instructions);

        final URI parentDir = uri("destDir");
        Table fromS3 = ParquetTools.readTable(parentDir.toString(), instructions);
        assertTableEquals(tableToSave, fromS3);

        // Try to write a bad table at the same destination. This write should fail midway and the original file should
        // be preserved.
        final Table badTable = TableTools.emptyTable(5)
                .updateView("InputString = ii % 2 == 0 ? Long.toString(ii) : null", "A=InputString.charAt(0)");
        try {
            ParquetTools.writeTable(badTable, fileUri.toString(), instructions);
            TestCase.fail("Exception expected for invalid formula");
        } catch (UncheckedDeephavenException e) {
            assertTrue(e.getCause() instanceof FormulaEvaluationException);
        }

        // Make sure that original file is preserved
        fromS3 = ParquetTools.readTable(parentDir.toString(), instructions);
        assertTableEquals(tableToSave, fromS3);
    }

    /**
     * These are tests for writing multiple parquet tables such that there is an exception in the second write.
     */
    @Test
    public void writeMultiTableExceptionTest() {
        // Write some initial data to S3
        final Table initialData = TableTools.emptyTable(5).update("A=(int)i", "B=(long)i", "C=(double)i");
        final URI initialDataUri = uri("destDir/initialDate.parquet");
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .build();
        ParquetTools.writeTable(initialData, initialDataUri.toString(), instructions);

        // Write two tables to parquet file and read them back
        final Table firstTable = TableTools.emptyTable(5)
                .updateView("InputString = Long.toString(ii)", "A=InputString.charAt(0)");
        final URI firstFileUri = uri("destDir/table1.parquet");
        final Table badTable = TableTools.emptyTable(5)
                .updateView("InputString = ii % 2 == 0 ? Long.toString(ii*5) : null", "A=InputString.charAt(0)");
        final URI secondFileUri = uri("destDir/table2.parquet");

        // This write should fail for the second table
        try {
            writeTables(new Table[] {firstTable, badTable},
                    new String[] {firstFileUri.toString(), secondFileUri.toString()}, instructions);
            TestCase.fail("Exception expected for invalid formula");
        } catch (UncheckedDeephavenException e) {
            assertTrue(e.getCause() instanceof FormulaEvaluationException);
        }

        // All new files should be deleted even though first table would be written successfully. The directory should
        // just have initial data.
        final URI parentDir = uri("destDir");
        final Table fromS3 = ParquetTools.readTable(parentDir.toString(), instructions);
        assertTableEquals(initialData, fromS3);
    }

    @Test
    public final void readFlatPartitionedParquetData()
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
        final Table table = getTable(100_000);
        final String destDirName = "flatPartitionedDataDir";
        final File destDir = new File(folder.newFolder(), destDirName);
        for (int i = 0; i < 3; ++i) {
            final File dest = new File(destDir, "table" + i + ".parquet");
            ParquetTools.writeTable(table, dest.getAbsolutePath());
        }
        final File pqFileToBeIgnored = new File(destDir, "temp/table.parquet");
        ParquetTools.writeTable(TableTools.emptyTable(100).select("someIntColumn = (int) i"),
                pqFileToBeIgnored.getAbsolutePath());
        uploadDirectory(destDir.toPath(), destDirName);
        final URI uri = uri(destDirName);
        final ParquetInstructions readInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .build();

        final Table expected = merge(table, table, table);
        final Table fromS3AsFlat = ParquetTools.readTable(uri.toString(),
                readInstructions.withLayout(ParquetInstructions.ParquetFileLayout.FLAT_PARTITIONED));
        assertTableEquals(expected, fromS3AsFlat);
    }

    @Test
    public final void readFlatPartitionedParquetDataAsKVPartitioned()
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
        final Table table = getTable(100_000);
        final String destDirName = "flatPartitionedDataDir";
        final File destDir = new File(folder.newFolder(), destDirName);
        for (int i = 0; i < 3; ++i) {
            final File dest = new File(destDir, "table" + i + ".parquet");
            ParquetTools.writeTable(table, dest.getAbsolutePath());
        }
        uploadDirectory(destDir.toPath(), destDirName);
        final URI uri = uri(destDirName);
        final ParquetInstructions readInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
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

        // Read with definition without layout
        final Table fromS3AsFlatWithDefinition = ParquetTools.readTable(uri.toString(),
                readInstructions.withTableDefinition(expected.getDefinition()));
        assertTableEquals(expected, fromS3AsFlatWithDefinition);
    }

    @Test
    public void readKeyValuePartitionedParquetData()
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
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
        final File destDir = new File(folder.newFolder(), destDirName);
        final ParquetInstructions writeInstructions = ParquetInstructions.builder()
                .setBaseNameForPartitionedParquetData("data")
                .build();
        writeKeyValuePartitionedTable(table, destDir.getPath(), writeInstructions);
        uploadDirectory(destDir.toPath(), destDirName);
        final URI uri = uri(destDirName);
        final ParquetInstructions readInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .setTableDefinition(definition)
                .build();
        final Table fromS3 = ParquetTools.readTable(uri.toString(), readInstructions);
        final Table fromDisk = ParquetTools.readTable(destDir.getPath());
        readPartitionedParquetTestHelper(fromDisk, fromS3);

        // Failure cases for missing metadata files
        try {
            ParquetTools.readTable(uri.toString(),
                    readInstructions.withTableDefinitionAndLayout(null,
                            ParquetInstructions.ParquetFileLayout.METADATA_PARTITIONED));
            fail("Expected exception because metadata file is not present");
        } catch (final TableDataException expected) {
            assertTrue(expected.getMessage().contains("metadata"));
        }
        final URI metadataFileURI = uri(destDirName + "/_metadata");
        try {
            ParquetTools.readTable(metadataFileURI.toString(), readInstructions.withTableDefinition(null));
            fail("Expected exception because metadata file is not present");
        } catch (final TableDataException expected) {
            assertTrue(expected.getMessage().contains("metadata"));
        }
    }

    @Test
    public void readWriteKeyValuePartitionedParquetData() {
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
        final URI uri = uri("keyValuePartitionedDataDir");
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .setTableDefinition(definition)
                .setBaseNameForPartitionedParquetData("data")
                .build();
        writeKeyValuePartitionedTable(table, uri.toString(), instructions);
        final Table fromS3 = ParquetTools.readTable(uri.toString(), instructions);
        assertTableEquals(table.sort("PC1", "PC2"), fromS3.sort("PC1", "PC2"));
    }

    @Test
    public void readMetadataPartitionedParquetData()
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
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
        final String destDirName = "metadataPartitionedDataDir";
        final File destDir = new File(folder.newFolder(), destDirName);
        final ParquetInstructions writeInstructions = ParquetInstructions.builder()
                .setBaseNameForPartitionedParquetData("data")
                .setGenerateMetadataFiles(true)
                .build();
        writeKeyValuePartitionedTable(table, destDir.getPath(), writeInstructions);
        assertTrue(new File(destDir, "_metadata").exists());
        assertTrue(new File(destDir, "_common_metadata").exists());
        uploadDirectory(destDir.toPath(), destDirName);
        final ParquetInstructions readInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .build();
        final URI directoryURI = uri(destDirName);
        final Table fromS3MetadataPartitioned = ParquetTools.readTable(directoryURI.toString(),
                readInstructions.withLayout(ParquetInstructions.ParquetFileLayout.METADATA_PARTITIONED));
        final Table fromDiskMetadataPartitioned = ParquetTools.readTable(destDir.getPath(),
                ParquetInstructions.EMPTY.withLayout(ParquetInstructions.ParquetFileLayout.METADATA_PARTITIONED));
        readPartitionedParquetTestHelper(fromDiskMetadataPartitioned, fromS3MetadataPartitioned);

        final URI metadataFileURI = uri(destDirName + "/_metadata");
        final Table fromS3WithMetadata = ParquetTools.readTable(metadataFileURI.toString(), readInstructions);
        final Table fromDiskWithMetadata = ParquetTools.readTable(new File(destDir, "_metadata").getPath());
        readPartitionedParquetTestHelper(fromDiskWithMetadata, fromS3WithMetadata);

        final URI commonMetadataFileURI = uri(destDirName + "/_common_metadata");
        final Table fromS3WithCommonMetadata =
                ParquetTools.readTable(commonMetadataFileURI.toString(), readInstructions);
        final Table fromDiskWithCommonMetadata =
                ParquetTools.readTable(new File(destDir, "_common_metadata").getPath());
        readPartitionedParquetTestHelper(fromDiskWithCommonMetadata, fromS3WithCommonMetadata);
    }

    private static void readPartitionedParquetTestHelper(final Table expected, final Table fromS3) {
        assertTrue(fromS3.getDefinition().getColumn("PC1").isPartitioning());
        assertTrue(fromS3.getDefinition().getColumn("PC2").isPartitioning());
        assertTableEquals(expected, fromS3);
    }

    @Test
    public void readMetadataPartitionedParquetWithMissingMetadataFile()
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
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
        final String destDirName = "metadataPartitionedDataDir";
        final File destDir = new File(folder.newFolder(), destDirName);
        final ParquetInstructions writeInstructions = ParquetInstructions.builder()
                .setBaseNameForPartitionedParquetData("data")
                .setGenerateMetadataFiles(true)
                .build();
        writeKeyValuePartitionedTable(table, destDir.getPath(), writeInstructions);

        // Delete the metadata file before uploading
        final File metadataFile = new File(destDir, "_metadata");
        metadataFile.delete();

        uploadDirectory(destDir.toPath(), destDirName);
        final URI directoryURI = uri(destDirName);
        final ParquetInstructions readInstructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .build();
        try {
            ParquetTools.readTable(directoryURI.toString(),
                    readInstructions.withLayout(ParquetInstructions.ParquetFileLayout.METADATA_PARTITIONED));
            fail("Expected exception because metadata file is not present");
        } catch (final TableDataException expected) {
            assertTrue(expected.getMessage().contains("metadata"));
        }
    }

    @Test
    public void readWriteMetadataPartitionedParquetData() {
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
        final URI uri = uri("keyValuePartitionedDataDir");
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .setTableDefinition(definition)
                .setBaseNameForPartitionedParquetData("data")
                .setGenerateMetadataFiles(true)
                .build();
        writeKeyValuePartitionedTable(table, uri.toString(), instructions);
        final Table fromS3 = ParquetTools.readTable(uri.toString(), instructions.withTableDefinitionAndLayout(null,
                ParquetInstructions.ParquetFileLayout.METADATA_PARTITIONED));
        assertTableEquals(table.sort("PC1", "PC2"), fromS3.sort("PC1", "PC2"));
    }

    @Test
    public void indexByLongKey() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("someInt"),
                ColumnDefinition.ofLong("someLong"));
        final Table testTable =
                ((QueryTable) TableTools.emptyTable(10).select("someInt = i", "someLong  = ii % 3")
                        .groupBy("someLong").ungroup("someInt")).withDefinitionUnsafe(definition);
        DataIndexer.getOrCreateDataIndex(testTable, "someLong");
        DataIndexer.getOrCreateDataIndex(testTable, "someInt", "someLong");

        final URI uri = uri("table.parquet");
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions(
                        S3Instructions.builder()
                                .readTimeout(Duration.ofSeconds(10)))
                        .build())
                .build();

        writeTable(testTable, uri.toString(), instructions);
        final Table fromS3 = ParquetTools.readTable(uri.toString(), instructions);
        assertTableEquals(testTable, fromS3);

        // Validate the indexes and lookup functions.
        verifyIndexingInfoExists(fromS3, "someLong");
        verifyIndexingInfoExists(fromS3, "someInt", "someLong");
        verifyIndexingInfoExists(fromS3, "someLong", "someInt");
    }

    @Test
    public void testReadWriteUsingProfile() throws IOException {
        final Table table = TableTools.emptyTable(5).update("someIntColumn = (int) i");
        Path tempConfigFile = null;
        Path tempCredentialsFile = null;
        try {
            // Create temporary config and credentials file and write wrong credentials to them
            tempConfigFile = Files.createTempFile("config", ".tmp");
            final String configData = "[profile test-user]\nregion = wrong-region";
            Files.write(tempConfigFile, configData.getBytes());

            tempCredentialsFile = Files.createTempFile("credentials", ".tmp");
            final String credentialsData = "[test-user]\naws_access_key_id = foo\naws_secret_access_key = bar";
            Files.write(tempCredentialsFile, credentialsData.getBytes());

            final S3Instructions s3Instructions = S3Instructions.builder()
                    .readTimeout(Duration.ofSeconds(3))
                    .endpointOverride(s3Endpoint())
                    .profileName("test-user")
                    .credentialsFilePath(tempCredentialsFile.toString())
                    .configFilePath(tempConfigFile.toString())
                    .credentials(Credentials.profile())
                    .build();
            final ParquetInstructions instructions = ParquetInstructions.builder()
                    .setSpecialInstructions(s3Instructions)
                    .build();
            try {
                final URI uri = uri("table1.parquet");
                ParquetTools.writeTable(table, uri.toString(), instructions);
                fail("Expected exception");
            } catch (final UncheckedDeephavenException expected) {
            }
        } finally {
            // Delete the temporary files
            if (tempConfigFile != null) {
                Files.deleteIfExists(tempConfigFile);
            }
            if (tempCredentialsFile != null) {
                Files.delete(tempCredentialsFile);
            }
        }

        try {
            // Create temporary config and credentials file and write correct credentials and region to them
            tempConfigFile = Files.createTempFile("config", ".tmp");
            final String configData = "[profile test-user]\nregion = " + region();
            Files.write(tempConfigFile, configData.getBytes());

            tempCredentialsFile = Files.createTempFile("credentials", ".tmp");
            final String credentialsData = "[test-user]\naws_access_key_id = " + accessKey() +
                    "\naws_secret_access_key = " + secretAccessKey();
            Files.write(tempCredentialsFile, credentialsData.getBytes());

            final S3Instructions s3Instructions = S3Instructions.builder()
                    .readTimeout(Duration.ofSeconds(3))
                    .endpointOverride(s3Endpoint())
                    .profileName("test-user")
                    .credentialsFilePath(tempCredentialsFile.toString())
                    .configFilePath(tempConfigFile.toString())
                    .credentials(Credentials.profile())
                    .build();
            final ParquetInstructions instructions = ParquetInstructions.builder()
                    .setSpecialInstructions(s3Instructions)
                    .build();
            final URI uri = uri("table2.parquet");
            ParquetTools.writeTable(table, uri.toString(), instructions);
            final Table fromS3 = ParquetTools.readTable(uri.toString(), instructions);
            assertTableEquals(table, fromS3);
        } finally {
            // Delete the temporary files
            Files.delete(tempConfigFile);
            Files.delete(tempCredentialsFile);
        }
    }
}
