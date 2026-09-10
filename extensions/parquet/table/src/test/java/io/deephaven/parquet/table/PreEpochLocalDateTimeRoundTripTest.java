//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 3, end to end: a pre-epoch {@link LocalDateTime} with a sub-second part could be written to parquet
 * but never read back.
 *
 * <p>
 * The write succeeded and the file was correct; the read threw
 * {@code java.time.DateTimeException: Invalid value for NanoOfSecond (valid values 0 - 999999999): -1} out of
 * {@code LocalDateTimeFromNanosMaterializer.convertValue}, which split the epoch offset with truncating {@code /} and
 * {@code %}. See {@code PreEpochLocalDateTimeMaterializerTest} in {@code extensions/parquet/base} for the unit-level
 * coverage of all three parquet precisions.
 *
 * <p>
 * Fuzzer case seed {@code 8750790217018904276L}.
 */
public class PreEpochLocalDateTimeRoundTripTest {

    private static final String ROOT_FILENAME = PreEpochLocalDateTimeRoundTripTest.class.getName() + "_root";

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

    private void assertRoundTrips(final String name, final LocalDateTime... values) {
        final Table source = TableTools.newTable(TableTools.col("Timestamp", values));
        final String dest = Path.of(rootFile.getPath(), name + ".parquet").toString();
        ParquetTools.writeTable(source, dest);

        // select() materializes every value, so a read failure surfaces here rather than lazily.
        final Table disk = ParquetTools.readTable(dest).select();
        assertEquals(values.length, disk.size());

        final ColumnSource<LocalDateTime> column = disk.getColumnSource("Timestamp");
        final List<LocalDateTime> actual = new ArrayList<>();
        disk.getRowSet().forAllRowKeys(rowKey -> actual.add(column.get(rowKey)));
        assertEquals(Arrays.asList(values), actual);
    }

    /** The exact value from the original failure. */
    @Test
    public void oneNanoBeforeEpoch() {
        assertRoundTrips("one_nano", LocalDateTime.parse("1969-12-31T23:59:59.999999999"));
    }

    /** Any pre-epoch value with a sub-second part, not just ones adjacent to the epoch. */
    @Test
    public void preEpochSubSecondValues() {
        assertRoundTrips("pre_epoch",
                LocalDateTime.parse("1969-12-31T23:59:59.999999999"),
                LocalDateTime.parse("1969-12-31T23:59:59.000000001"),
                LocalDateTime.parse("1900-06-15T12:30:00.123456789"),
                LocalDateTime.parse("1678-01-01T00:00:00.000000001"));
    }

    /** Pre-epoch whole seconds worked before the fix; post-epoch values are the control. */
    @Test
    public void wholeSecondsAndPostEpochUnchanged() {
        assertRoundTrips("controls",
                LocalDateTime.parse("1969-12-31T23:59:59"),
                LocalDateTime.parse("1970-01-01T00:00:00"),
                LocalDateTime.parse("2000-01-01T00:00:00.000000001"),
                LocalDateTime.parse("2261-12-31T23:59:59.999999999"));
    }

    /** Nulls interleaved with pre-epoch values, the shape the fuzzer actually generated. */
    @Test
    public void preEpochWithNulls() {
        assertRoundTrips("with_nulls",
                LocalDateTime.parse("1969-12-31T23:59:59.999999999"),
                null,
                LocalDateTime.parse("1900-06-15T12:30:00.123456789"),
                null);
    }

    /** A filter over the read-back column, which is how the fuzzer reached it. */
    @Test
    public void filterOverPreEpochValues() {
        final Table source = TableTools.newTable(TableTools.col("Timestamp",
                LocalDateTime.parse("1969-12-31T23:59:59.999999999"),
                LocalDateTime.parse("1970-01-01T00:00:00"),
                LocalDateTime.parse("1900-06-15T12:30:00.123456789")));
        final String dest = Path.of(rootFile.getPath(), "filtered.parquet").toString();
        ParquetTools.writeTable(source, dest);

        final Table disk = ParquetTools.readTable(dest);
        assertEquals(2, disk.where("Timestamp < '1970-01-01T00:00:00'").size());
        assertEquals(3, disk.select().size());
    }
}
