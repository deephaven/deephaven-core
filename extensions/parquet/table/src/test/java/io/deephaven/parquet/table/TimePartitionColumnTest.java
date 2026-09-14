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
import java.time.Instant;
import java.time.LocalTime;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 12: a key-value partitioned write whose partition value contains a colon — {@code LocalTime},
 * {@code Instant} — failed before writing anything:
 *
 * <pre>
 * UncheckedDeephavenException: Failed to create URI from relative path: Col0=2023-11-14T22:13:21Z/
 *   caused by: URISyntaxException: Illegal character in scheme name at index 4: Col0=2023-11-14T22:13:21Z/
 * </pre>
 *
 * <p>
 * {@code ParquetUtils.resolve} built the partition directory's relative URI with
 * {@code new URI(null, null, relativePath, null)}. RFC 3986 section 4.2: a path segment containing a colon "cannot be
 * used as the first segment of a relative-path reference, as it would be mistaken for a scheme name. Such a segment
 * must be preceded by a dot-segment". Without that prefix the parser reads {@code Col0} as the start of a scheme and
 * rejects the {@code =} at index 4.
 *
 * <p>
 * Both types are in {@code PartitionFormatter}'s and {@code PartitionParser}'s type maps, so both are claimed as
 * supported partitioning types; neither could actually be used.
 *
 * <p>
 * Fuzzer seeds {@code -2281078010550439077L} ({@code Instant}, recorded as finding 7d in {@code OLD_FINDINGS.md}),
 * {@code -1767017146706312469L} ({@code Instant}) and {@code 3579704455286775782L} ({@code LocalTime}, recorded in that
 * file's write-path table).
 */
public class TimePartitionColumnTest {

    private static final String ROOT_FILENAME = TimePartitionColumnTest.class.getName() + "_root";

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

    private String dir(final String name) {
        final File d = Path.of(rootFile.getPath(), name).toFile();
        // noinspection ResultOfMethodCallIgnored
        d.mkdirs();
        return d.getPath();
    }

    /** The {@code LocalTime} case: {@code Key=00:00:02}. */
    @Test
    public void localTimePartitionColumn() {
        final String dest = dir("local_time");
        final Table source = TableTools.newTable(
                TableTools.col("Key", LocalTime.parse("00:00:02"), LocalTime.parse("00:00:02"),
                        LocalTime.parse("12:34:56.789")),
                TableTools.intCol("Value", 1, 2, 3));
        ParquetTools.writeKeyValuePartitionedTable(source.partitionBy("Key"), dest, ParquetInstructions.EMPTY);

        final Table disk = ParquetTools.readTable(dest);
        assertEquals(3, disk.size());
        assertEquals(LocalTime.class, disk.getDefinition().getColumn("Key").getDataType());
        assertEquals(2, disk.where("Key == '00:00:02'").size());
        assertEquals(3, disk.select().size());
    }

    /** The {@code Instant} case: {@code Key=2023-11-14T22:13:21Z}. */
    @Test
    public void instantPartitionColumn() {
        final String dest = dir("instant");
        final Table source = TableTools.newTable(
                TableTools.col("Key", Instant.parse("2023-11-14T22:13:21Z"),
                        Instant.parse("2023-11-14T22:13:21Z"), Instant.parse("2024-02-29T12:00:00Z")),
                TableTools.intCol("Value", 1, 2, 3));
        ParquetTools.writeKeyValuePartitionedTable(source.partitionBy("Key"), dest, ParquetInstructions.EMPTY);

        final Table disk = ParquetTools.readTable(dest);
        assertEquals(3, disk.size());
        assertEquals(Instant.class, disk.getDefinition().getColumn("Key").getDataType());
        assertEquals(2, disk.where("Key == '2023-11-14T22:13:21Z'").size());
    }

    /** Sub-second precision, so the formatted value has both colons and a dot. */
    @Test
    public void instantWithSubSecondPrecision() {
        final String dest = dir("instant_nanos");
        final Table source = TableTools.newTable(
                TableTools.col("Key", Instant.parse("2023-11-14T22:13:21.123456789Z"),
                        Instant.parse("2024-02-29T12:00:00.000000001Z")),
                TableTools.intCol("Value", 1, 2));
        ParquetTools.writeKeyValuePartitionedTable(source.partitionBy("Key"), dest, ParquetInstructions.EMPTY);
        assertEquals(2, ParquetTools.readTable(dest).size());
    }

    /** Two partitioning levels, so a colon appears in a segment that is not the first. */
    @Test
    public void twoPartitioningLevelsWithColons() {
        final String dest = dir("two_levels");
        final Table source = TableTools.newTable(
                TableTools.stringCol("Outer", "aa", "aa", "bb"),
                TableTools.col("Inner", LocalTime.parse("00:00:02"), LocalTime.parse("01:02:03"),
                        LocalTime.parse("00:00:02")),
                TableTools.intCol("Value", 1, 2, 3));
        ParquetTools.writeKeyValuePartitionedTable(source.partitionBy("Outer", "Inner"), dest,
                ParquetInstructions.EMPTY);

        final Table disk = ParquetTools.readTable(dest);
        assertEquals(3, disk.size());
        assertEquals(2, disk.where("Outer == `aa`").size());
        assertEquals(2, disk.where("Inner == '00:00:02'").size());
    }

    /** With metadata files, which resolve additional paths through the same helper. */
    @Test
    public void localTimePartitionColumnWithMetadataFiles() {
        final String dest = dir("local_time_meta");
        final Table source = TableTools.newTable(
                TableTools.col("Key", LocalTime.parse("00:00:02"), LocalTime.parse("12:34:56.789")),
                TableTools.intCol("Value", 1, 2));
        ParquetTools.writeKeyValuePartitionedTable(source.partitionBy("Key"), dest,
                new ParquetInstructions.Builder().setGenerateMetadataFiles(true).build());
        assertEquals(2, ParquetTools.readTable(dest).size());
    }

    /** Colon-free partition values must be unaffected: no dot-segment, same layout as before. */
    @Test
    public void colonFreePartitionValuesUnchanged() {
        final String dest = dir("plain");
        final Table source = TableTools.newTable(
                TableTools.stringCol("Key", "aa", "aa", "bb"),
                TableTools.intCol("Value", 1, 2, 3));
        ParquetTools.writeKeyValuePartitionedTable(source.partitionBy("Key"), dest, ParquetInstructions.EMPTY);

        final Table disk = ParquetTools.readTable(dest);
        assertEquals(3, disk.size());
        assertEquals(2, disk.where("Key == `aa`").size());
        final String[] contents = new File(dest).list();
        assertEquals(2, contents == null ? -1 : contents.length);
    }
}
