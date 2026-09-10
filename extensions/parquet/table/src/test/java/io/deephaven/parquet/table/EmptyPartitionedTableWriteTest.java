//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * DH-23557 finding 4: a key-value partitioned write of a table with <em>no partitions</em> crashed inside the writer
 * with {@code ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0}.
 *
 * <p>
 * {@code writeTablesImpl} reads {@code destinations[0].getScheme()} to pick a channels provider. Every other caller
 * guarantees a non-empty array -- the public {@code writeTables} rejects an empty one outright -- but
 * {@code writeKeyValuePartitionedTableImpl} derives its destinations from {@code partitionBy(...)}, which yields no
 * constituents for an empty table, so it alone could hand down zero.
 *
 * <p>
 * Writing nothing when there is nothing to write is the right outcome: an incremental pipeline that partitions by day
 * must not fail on a day with no rows. Reading the resulting empty directory back still fails, with the clear and
 * intentional {@code "Unable to infer schema for a partitioned parquet table when there are no initial parquet files"}
 * -- the key-value layout records the schema only in its data files, so an empty directory carries none. That is a
 * property of the format, not a defect, and it is asserted here so the distinction stays recorded.
 *
 * <p>
 * Fuzzer case seed {@code -3203144279381597024L}; 7 of the 29 failures in the run that found it.
 */
public class EmptyPartitionedTableWriteTest {

    private static final String ROOT_FILENAME = EmptyPartitionedTableWriteTest.class.getName() + "_root";

    private ExecutionContext executionContext;
    private SafeCloseable executionContextCloseable;
    private File rootFile;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContextCloseable = executionContext.open();
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        if (rootFile != null) {
            FileUtils.deleteRecursively(rootFile);
        }
        executionContextCloseable.close();
    }

    private static Table emptySource() {
        return TableTools.newTable(TableTools.stringCol("Key"), TableTools.intCol("Value"));
    }

    private String dir(final String name) {
        final File d = Path.of(rootFile.getPath(), name).toFile();
        // noinspection ResultOfMethodCallIgnored
        d.mkdirs();
        return d.getPath();
    }

    /** The defect: this threw ArrayIndexOutOfBoundsException from inside the writer. */
    @Test
    public void writeEmptyPartitionedTableIsANoOp() {
        final String dest = dir("empty");
        ParquetTools.writeKeyValuePartitionedTable(emptySource().partitionBy("Key"), dest, ParquetInstructions.EMPTY);

        final String[] contents = new File(dest).list();
        assertNotNull(contents);
        assertEquals("no partitions written", 0, contents.length);
    }

    /** Same, with metadata files requested: still nothing to describe, still no crash. */
    @Test
    public void writeEmptyPartitionedTableWithMetadataFilesIsANoOp() {
        final String dest = dir("empty_meta");
        ParquetTools.writeKeyValuePartitionedTable(emptySource().partitionBy("Key"), dest,
                new ParquetInstructions.Builder().setGenerateMetadataFiles(true).build());

        final String[] contents = new File(dest).list();
        assertNotNull(contents);
        assertEquals("no metadata for zero files", 0, contents.length);
    }

    /**
     * The read-back limitation, asserted so it stays a deliberate contract rather than a surprise: the failure must be
     * the clear schema-inference message, not the internal crash this finding was about.
     */
    @Test
    public void readingBackAnEmptyPartitionedDirectoryReportsMissingSchema() {
        final String dest = dir("empty_read");
        ParquetTools.writeKeyValuePartitionedTable(emptySource().partitionBy("Key"), dest, ParquetInstructions.EMPTY);

        final IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> ParquetTools.readTable(dest));
        assertTrue(thrown.getMessage(), thrown.getMessage().contains("Unable to infer schema"));
    }

    /** A non-empty partitioned write is unaffected. */
    @Test
    public void nonEmptyPartitionedWriteIsUnchanged() {
        final String dest = dir("populated");
        // Multi-character keys deliberately: a single-character String partitioning value is parsed back as a
        // char, which is finding 5 and not what this test is about.
        final Table source = TableTools.newTable(
                TableTools.stringCol("Key", "aa", "aa", "bb"),
                TableTools.intCol("Value", 1, 2, 3));
        ParquetTools.writeKeyValuePartitionedTable(source.partitionBy("Key"), dest, ParquetInstructions.EMPTY);

        final Table disk = ParquetTools.readTable(dest);
        assertEquals(3, disk.size());
        assertEquals(2, disk.where("Key == `aa`").size());
    }

    /**
     * Writing an empty partitioned table over an already-populated directory leaves it intact and readable. This is the
     * incremental-pipeline case that argues for the no-op over an exception.
     */
    @Test
    public void emptyWriteOverPopulatedDirectoryLeavesItReadable() {
        final String dest = dir("incremental");
        ParquetTools.writeKeyValuePartitionedTable(TableTools.newTable(
                TableTools.stringCol("Key", "aa", "bb"),
                TableTools.intCol("Value", 1, 2)).partitionBy("Key"), dest, ParquetInstructions.EMPTY);
        ParquetTools.writeKeyValuePartitionedTable(emptySource().partitionBy("Key"), dest, ParquetInstructions.EMPTY);

        final Table disk = ParquetTools.readTable(dest);
        assertEquals(2, disk.size());
    }
}
