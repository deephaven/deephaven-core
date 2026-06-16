//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.testutil.sources.InstantTestSource;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.util.QueryConstants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static org.junit.Assert.assertThrows;

public class TestTailInitializationFilter extends RefreshingTableTestCase {

    public void testSimple() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 99);
        builder.appendRange(1000, 1099);
        final long[] data = new long[200];
        final Instant baseTime = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
        final Instant baseTime2 = DateTimeUtils.parseInstant("2020-08-20T06:00:00 NY");
        for (int ii = 0; ii < 100; ii++) {
            data[ii] = DateTimeUtils.epochNanos(baseTime) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
            data[100 + ii] = DateTimeUtils.epochNanos(baseTime2) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
        }
        final Instant threshold1 = DateTimeUtils.epochNanosToInstant(data[99] - DateTimeUtils.secondsToNanos(600));
        final Instant threshold2 = DateTimeUtils.epochNanosToInstant(data[199] - DateTimeUtils.secondsToNanos(600));

        final QueryTable input = TstUtils.testRefreshingTable(builder.build().toTracking(),
                ColumnHolder.getInstantColumnHolder("Timestamp", false, data));
        final Table filtered = TailInitializationFilter.mostRecent(input.assertAddOnly(), "Timestamp", "PT00:10:00");
        assertTrue(((BaseTable) filtered).isAddOnly());
        assertEquals(44, filtered.size());

        final Table slice0_100_filtered = input.slice(0, 100).where("Timestamp >= '" + threshold1 + "'");
        final Table slice100_200_filtered = input.slice(100, 200).where("Timestamp >= '" + threshold2 + "'");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Table expected = updateGraph.sharedLock().computeLocked(
                () -> TableTools.merge(slice0_100_filtered, slice100_200_filtered));
        assertTableEquals(filtered, expected);

        updateGraph.runWithinUnitTestCycle(() -> {
            final Instant[] data2 = new Instant[4];
            data2[0] = DateTimeUtils.parseInstant("2020-08-20T06:00:00 NY");
            data2[1] = DateTimeUtils.parseInstant("2020-08-20T06:30:00 NY");
            data2[2] = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
            data2[3] = DateTimeUtils.parseInstant("2020-08-20T08:30:00 NY");
            final RowSet newRowSet = RowSetFactory.fromKeys(100, 101, 1100, 1101);
            input.getRowSet().writableCast().insert(newRowSet);
            ((InstantTestSource) input.<Instant>getColumnSource("Timestamp")).add(newRowSet, data2);
            input.notifyListeners(newRowSet, i(), i());
        });

        final Table slice100_102 = input.slice(100, 102);
        final Table slice102_202_filtered = input.slice(102, 202).where("Timestamp >= '" + threshold2 + "'");
        final Table slice202_204 = input.slice(202, 204);
        final Table expected2 = updateGraph.sharedLock().computeLocked(
                () -> TableTools.merge(slice0_100_filtered, slice100_102, slice102_202_filtered, slice202_204));
        assertTableEquals(filtered, expected2);
    }

    public void testMostRecentRows() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 99);
        builder.appendRange(1000, 1099);
        final long[] data = new long[200];
        final Instant baseTime = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
        final Instant baseTime2 = DateTimeUtils.parseInstant("2020-08-20T06:00:00 NY");
        for (int ii = 0; ii < 100; ii++) {
            data[ii] = DateTimeUtils.epochNanos(baseTime) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
            data[100 + ii] = DateTimeUtils.epochNanos(baseTime2) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
        }
        final QueryTable input = TstUtils.testRefreshingTable(builder.build().toTracking(),
                ColumnHolder.getInstantColumnHolder("Timestamp", false, data));

        final Table filtered = TailInitializationFilter.mostRecentRows(input.assertAddOnly(), 10);
        assertTrue(((BaseTable) filtered).isAddOnly());
        assertEquals(20, filtered.size());

        final Table slice0 = input.slice(90, 100);
        final Table slice1 = input.slice(190, 200);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Table expected = updateGraph.sharedLock().computeLocked(
                () -> TableTools.merge(slice0, slice1));
        assertTableEquals(filtered, expected);

        updateGraph.runWithinUnitTestCycle(() -> {
            final Instant[] data2 = new Instant[4];
            data2[0] = DateTimeUtils.parseInstant("2020-08-20T06:00:00 NY");
            data2[1] = DateTimeUtils.parseInstant("2020-08-20T06:30:00 NY");
            data2[2] = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
            data2[3] = DateTimeUtils.parseInstant("2020-08-20T08:30:00 NY");
            final RowSet newRowSet = RowSetFactory.fromKeys(100, 101, 1100, 1101);
            input.getRowSet().writableCast().insert(newRowSet);
            ((InstantTestSource) input.<Instant>getColumnSource("Timestamp")).add(newRowSet, data2);
            input.notifyListeners(newRowSet, i(), i());
        });

        final Table slice0a = input.slice(90, 100);
        final Table slice1a = input.slice(192, 202);
        final Table slice0b = input.slice(100, 102);
        final Table slice1b = input.slice(202, 204);
        final Table expected2 = updateGraph.sharedLock().computeLocked(
                () -> TableTools.merge(slice0a, slice0b, slice1a, slice1b));
        assertTableEquals(filtered, expected2);

        // let's use a rowcount of 1 to check for some edge conditions
        final Table filtered1 = TailInitializationFilter.mostRecentRows(input.assertAddOnly(), 1);
        assertEquals(2, filtered1.size());
        final Table expected3 = TableTools.merge(input.slice(101, 102), input.slice(203, 204));
        assertTableEquals(filtered1, expected3);

        // and of course zero is always fun
        final Table filtered0 = TailInitializationFilter.mostRecentRows(input.assertAddOnly(), 0);
        assertEquals(0, filtered0.size());
        assertTableEquals(filtered0, TableTools.newTable(input.getDefinition()));
    }

    public void testBadAdds() {
        final QueryTable input =
                TstUtils.testRefreshingTable(ColumnHolder.getInstantColumnHolder("Timestamp", false, new long[0]));
        final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> TailInitializationFilter.mostRecent(input, "Timestamp", "PT00:10:00"));
        assertEquals("TailInitializationFilter requires an add-only table as input.", iae.getMessage());
        final IllegalArgumentException iae2 =
                assertThrows(IllegalArgumentException.class, () -> TailInitializationFilter.mostRecentRows(input, 1));
        assertEquals("TailInitializationFilter requires an add-only table as input.", iae2.getMessage());
    }

    public void testNoReinterpret() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 99);
        builder.appendRange(1000, 1099);
        final long[] data = new long[200];
        final Instant baseTime = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
        final Instant baseTime2 = DateTimeUtils.parseInstant("2020-08-20T06:00:00 NY");
        for (int ii = 0; ii < 100; ii++) {
            data[ii] = DateTimeUtils.epochNanos(baseTime) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
            data[100 + ii] = DateTimeUtils.epochNanos(baseTime2) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
        }

        final QueryTable input = TstUtils.testRefreshingTable(builder.build().toTracking(),
                ColumnHolder.getInstantColumnHolder("Timestamp", false, data));
        final Table filtered = TailInitializationFilter.mostRecent(
                input.view("Timestamp=Timestamp - 'PT1s' + 'PT1s'").assertAddOnly(), "Timestamp", "PT00:10:00");
        assertTrue(((BaseTable) filtered).isAddOnly());
        assertEquals(44, filtered.size());
    }

    public void testNullValues() throws IOException {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 99);
        builder.appendRange(1000, 1099);
        final long[] data = new long[200];
        final Instant baseTime = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
        final Instant baseTime2 = DateTimeUtils.parseInstant("2020-08-20T06:00:00 NY");
        for (int ii = 0; ii < 100; ii++) {
            data[ii] = DateTimeUtils.epochNanos(baseTime) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
            data[100 + ii] = DateTimeUtils.epochNanos(baseTime2) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
        }

        final TrackingWritableRowSet rowset = builder.build().toTracking();
        final QueryTable input = TstUtils.testRefreshingTable(rowset,
                ColumnHolder.getInstantColumnHolder("Timestamp", false, data));
        final Table filtered = TailInitializationFilter.mostRecent(
                input.assertAddOnly(), "Timestamp", "PT00:10:00");
        assertEquals(44, filtered.size());

        // now let's break some data
        final long[] baddata = Arrays.copyOf(data, data.length);
        baddata[99] = QueryConstants.NULL_LONG;
        doNullCheck(rowset, baddata, "Found null timestamp at row key 99");

        baddata[99] = data[99];
        baddata[50] = QueryConstants.NULL_LONG;
        doNullCheck(rowset, baddata, "Found null timestamp at row key 50");

        baddata[50] = data[50];
        baddata[0] = QueryConstants.NULL_LONG;
        doNullCheck(rowset, baddata, "Found null timestamp at row key 0");
    }

    private static void doNullCheck(TrackingWritableRowSet rowset, long[] baddata, String expected) throws IOException {
        final QueryTable badinput = TstUtils.testRefreshingTable(rowset,
                ColumnHolder.getInstantColumnHolder("Timestamp", false, baddata));
        final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> TailInitializationFilter.mostRecent(badinput.assertAddOnly(), "Timestamp", "PT00:10:00"));
        assertEquals(expected, iae.getMessage());

        final Table badsnap1 = badinput.slice(0, 100).snapshot();

        final Path tempDirectory = Files.createTempDirectory("testNullValues");
        try {
            final File parquetFile = tempDirectory.resolve("test1.parquet").toAbsolutePath().toFile();
            ParquetTools.writeTable(badsnap1, parquetFile.toString());

            final Table badParquet1 = ParquetTools.readTable(parquetFile.toString());
            final IllegalArgumentException iae4 = assertThrows(IllegalArgumentException.class,
                    () -> TailInitializationFilter.mostRecent(badParquet1, "Timestamp", "PT00:10:00"));
            assertEquals(expected, iae4.getMessage());
        } finally {
            FileUtils.deleteRecursively(tempDirectory.toFile());
        }
    }

    public void testOutOfOrder() throws IOException {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 99);
        final long[] data = new long[100];
        final Instant baseTime = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
        for (int ii = 0; ii < 100; ii++) {
            data[ii] = DateTimeUtils.epochNanos(baseTime) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
        }

        final QueryTable input = TstUtils.testRefreshingTable(builder.build().toTracking(),
                ColumnHolder.getInstantColumnHolder("Timestamp", false, data));
        final Table filtered = TailInitializationFilter.mostRecent(input.assertAddOnly(), "Timestamp", "PT00:10:00");
        assertEquals(22, filtered.size());

        // now let's break some data in interesting ways to force out-of-order conditions
        final long[] baddata = Arrays.copyOf(data, data.length);
        baddata[0] = data[99] + 1;
        outOfOrderTest(builder, baddata, "Found inconsistently sorted rows, " + baddata[0]
                + " at row key 0 is greater than " + baddata[99] + " at row key 99");
        baddata[0] = data[0];

        baddata[75] = data[99] + 1;
        outOfOrderTest(builder, baddata, "Found inconsistently sorted rows, " + baddata[75]
                + " at row key 75 is greater than " + baddata[99] + " at row key 99");
        baddata[75] = data[75];

        baddata[88] = data[99] + 1;
        outOfOrderTest(builder, baddata, "Found inconsistently sorted rows, " + baddata[88]
                + " at row key 88 is greater than " + baddata[99] + " at row key 99");
        baddata[88] = data[87];

        baddata[50] = data[0] - 1;
        outOfOrderTest(builder, baddata, "Found inconsistently sorted rows, " + baddata[50]
                + " at row key 50 is less than " + baddata[0] + " at row key 0");
        baddata[50] = data[0];

        baddata[88] = data[75] - 1;
        outOfOrderTest(builder, baddata, "Found inconsistently sorted rows, " + baddata[88]
                + " at row key 88 is less than " + baddata[75] + " at row key 75");
        baddata[88] = data[88];

        baddata[82] = data[88] + 1;
        outOfOrderTest(builder, baddata, "Found inconsistently sorted rows, " + baddata[82]
                + " at row key 82 is greater than " + baddata[88] + " at row key 88");
        baddata[82] = data[82];
    }

    private static void outOfOrderTest(RowSetBuilderSequential builder, long[] baddata, final String expected)
            throws IOException {
        final QueryTable badinput = TstUtils.testRefreshingTable(builder.build().toTracking(),
                ColumnHolder.getInstantColumnHolder("Timestamp", false, baddata));
        final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> TailInitializationFilter.mostRecent(badinput.assertAddOnly(), "Timestamp", "PT00:10:00"));
        assertEquals(expected, iae.getMessage());

        final Path tempDirectory = Files.createTempDirectory("testOutOfOrder");
        try {
            final File parquetFile = tempDirectory.resolve("test1.parquet").toAbsolutePath().toFile();
            ParquetTools.writeTable(badinput, parquetFile.toString());

            final Table badParquet = ParquetTools.readTable(parquetFile.toString());
            final IllegalArgumentException iae4 = assertThrows(IllegalArgumentException.class,
                    () -> TailInitializationFilter.mostRecent(badParquet, "Timestamp", "PT00:10:00"));
            assertEquals(expected, iae4.getMessage());
        } finally {
            FileUtils.deleteRecursively(tempDirectory.toFile());
        }
    }


    public void testSimpleRegioned() throws IOException {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 99);
        builder.appendRange(1000, 1099);
        final long[] data = new long[200];
        final Instant baseTime = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
        final Instant baseTime2 = DateTimeUtils.parseInstant("2020-08-20T06:00:00 NY");
        for (int ii = 0; ii < 100; ii++) {
            data[ii] = DateTimeUtils.epochNanos(baseTime) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
            data[100 + ii] = DateTimeUtils.epochNanos(baseTime2) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
        }
        final Instant threshold1 = DateTimeUtils.epochNanosToInstant(data[99] - DateTimeUtils.secondsToNanos(600));
        final Instant threshold2 = DateTimeUtils.epochNanosToInstant(data[199] - DateTimeUtils.secondsToNanos(600));


        final QueryTable toWrite = TstUtils.testRefreshingTable(builder.build().toTracking(),
                ColumnHolder.getInstantColumnHolder("Timestamp", false, data));

        Path tempDirectory = Files.createTempDirectory("testSimpleRegioned");

        try {
            ParquetTools.writeTable(toWrite.slice(0, 100),
                    tempDirectory.resolve("test1.parquet").toAbsolutePath().toString());
            ParquetTools.writeTable(toWrite.slice(100, 200),
                    tempDirectory.resolve("test2.parquet").toAbsolutePath().toString());
            final Table input = ParquetTools.readTable(tempDirectory.toFile().getAbsolutePath());

            final Table result1 = TailInitializationFilter.mostRecent(input, "Timestamp", "PT00:10:00");
            assertFalse(result1.isRefreshing());
            assertEquals(44, result1.size());

            final Table slice0_100_filtered = input.slice(0, 100).where("Timestamp >= '" + threshold1 + "'");
            final Table slice100_200_filtered = input.slice(100, 200).where("Timestamp >= '" + threshold2 + "'");
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            final Table expected1 = updateGraph.sharedLock().computeLocked(
                    () -> TableTools.merge(slice0_100_filtered, slice100_200_filtered));
            assertTableEquals(result1, expected1);

            final Table preFilter = input.where(WhereFilterFactory.getExpression("ii % 2 == 0").withSerial());
            final Table result2 = TailInitializationFilter.mostRecent(preFilter, "Timestamp", "PT00:10:00");

            final Table slice0_100_filtered2 = preFilter.slice(0, 50).where("Timestamp >= '" + threshold1 + "'");
            final Table slice100_200_filtered2 = preFilter.slice(50, 100).where("Timestamp >= '" + threshold2 + "'");
            final Table expected2 = updateGraph.sharedLock().computeLocked(
                    () -> TableTools.merge(slice0_100_filtered2, slice100_200_filtered2));
            assertTableEquals(result2, expected2);

            assertEquals(22, result2.size());

            final Table byRows = TailInitializationFilter.mostRecentRows(input, 5);
            assertEquals(10, byRows.size());
            final Table expectedByRows = updateGraph.sharedLock().computeLocked(
                    () -> TableTools.merge(toWrite.slice(95, 100), toWrite.slice(195, 200)));
            assertTableEquals(expectedByRows, byRows);

            final Table preFilterByRows = TailInitializationFilter.mostRecentRows(preFilter, 5);

            final Table expectedPreFilterRows = updateGraph.sharedLock().computeLocked(
                    () -> TableTools.merge(preFilter.slice(45, 50), preFilter.slice(95, 100)));
            assertTableEquals(expectedPreFilterRows, preFilterByRows);

            final Table byRowsAll = TailInitializationFilter.mostRecentRows(input, 200);
            assertTableEquals(input, byRowsAll);
        } finally {
            FileUtils.deleteRecursively(tempDirectory.toFile());
        }
    }
}
