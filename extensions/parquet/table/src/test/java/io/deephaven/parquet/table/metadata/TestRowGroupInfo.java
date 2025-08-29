//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.parquet.table.ParquetTableFilterTest;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.deephaven.engine.util.TableTools.merge;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

/**
 * Test each of the {@link RowGroupInfo} implementations. These tests do not write parquet; they check that the expected
 * RowGroups (iterated Tables) are returned
 */
@SuppressWarnings("SameParameterValue")
public class TestRowGroupInfo {
    private static final String ROOT_FILENAME = TestRowGroupInfo.class.getName() + "_root";
    private static final String[] groupCol = {"random_int"};

    private static final ExecutionContext executionContext = TestExecutionContext.createForUnitTests();
    @SuppressWarnings("unused")
    private static final SafeCloseable executionContextCloseable = executionContext.open();

    private static File rootFile;
    private static Table testTable;

    @BeforeClass
    public static void setUp() {
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();

        final String dirPath = ParquetTableFilterTest.class.getResource("/parquet_no_stat").getFile();

        testTable = ParquetTools.readTable(dirPath);
    }

    @AfterClass
    public static void tearDown() {
        FileUtils.deleteRecursively(rootFile);
    }


    /**
     * A helper method which gets a collection of {@link Table}s for a given {@link RowGroupInfo}. Each {@link Table}
     * within the collection represents a single RowGroup
     *
     * @param input the input table to use for the test
     * @param rgi a {@link RowGroupInfo} to test
     * @return a collection of {@link Table}s where each member represents a RowGroup
     */
    private static List<Table> getRowGroups(final @NotNull Table input, final @NotNull RowGroupInfo rgi) {
        final List<Table> rowGroups = new ArrayList<>();

        try {
            rgi.applyForRowGroups(input, rowGroups::add);
        } catch (final IOException ioe) {
            // we're adding to a collection, which should not IOE. but we syntactically need to handle the exception
            throw new RuntimeException(ioe);
        }

        assertEquals("Sum of RowGroups equals Table.size()", input.size(), merge(rowGroups).size());

        return rowGroups;
    }

    /**
     * Verify that a "SingleRowGroup" results in ... a single RowGroup
     */
    @Test
    public void testSingleRowGroup() {
        final List<Table> rowGroups = getRowGroups(testTable, RowGroupInfo.singleRowGroup());
        assertEquals("singleRowGroup returns single RowGroup", 1, rowGroups.size());
    }

    /**
     * Verify that "SplitEvenly" results in ... a proper number of ~evenly split RowGroups
     *
     * @param input the input table to split
     * @param numRowGroups the desired number of RowGroups
     */
    private static void assertSplitEvenly(final @NotNull Table input, long numRowGroups) {
        final List<Table> rowGroups = getRowGroups(input, RowGroupInfo.splitEvenly(numRowGroups));
        final String totalMsg = String.format("splitEvenly(%d) returns %d RowGroups", numRowGroups, numRowGroups);
        assertEquals(totalMsg, numRowGroups, rowGroups.size()); // we have the expected number of RowGroups

        final long impliedSize = testTable.size() / numRowGroups;
        final long frontLoaded = testTable.size() % numRowGroups;

        // each RowGroup must be `impliedSize` (or `impliedSize+1` for the first `frontLoaded` RowGroups)
        for (int ii = 0; ii < rowGroups.size(); ii++) {
            final long subSize = rowGroups.get(ii).size();
            final long expectedSize = impliedSize + (ii < frontLoaded ? 1 : 0);
            final String subMsg =
                    String.format("splitEvenly(%d) size %d rows for RowGroup[%d]", numRowGroups, expectedSize, ii);
            assertEquals(subMsg, expectedSize, subSize);
        }
    }

    /**
     * Verify that "SplitEvenly" results in ... a proper number of ~evenly split RowGroups
     */
    @Test
    public void testSplitEvenly() {
        assertSplitEvenly(testTable, 1); // should shortcut away and use "SingleRowGroup" (iterator)
        assertSplitEvenly(testTable, 10);
        assertSplitEvenly(testTable, 11);
        assertSplitEvenly(testTable, 1000);
    }

    /**
     * A helper method to verify that ... each RowGroup contains {@code maxRows} (or fewer) rows
     *
     * @param rowGroups the {@link Table} representation of each RowGroup
     * @param maxRows the maximum number of rows permitted for each of the {@code rowGroups}
     */
    private static void assertRowGroupSizes(final @NotNull List<Table> rowGroups, final long maxRows) {
        for (int ii = 0; ii < rowGroups.size(); ii++) {
            final String msg =
                    String.format("withMaxRows(%d) has %d or fewer rows for RowGroup[%d]", maxRows, maxRows, ii);
            assertTrue(msg, rowGroups.get(ii).size() <= maxRows);
        }
    }

    /**
     * Verify that "MaxRows" results in ... a number of RowGroups, each of which contain `maxRows` (or fewer) rows
     *
     * @param input the input table to split
     * @param maxRows the maximum number of rows permitted for each RowGroup
     * @param expectedRowGroups the expected number of RowGroups that should be defined
     */
    private static void assertMaxRows(final @NotNull Table input, long maxRows, long expectedRowGroups) {
        final List<Table> rowGroups = getRowGroups(input, RowGroupInfo.withMaxRows(maxRows));
        final long calcdRowGroups = (input.size() / maxRows) + (input.size() % maxRows > 0 ? 1 : 0);
        assertEquals("Expected RowGroups matches Calculated RowGroups", expectedRowGroups, calcdRowGroups);
        final String msg = String.format("withMaxRows(%d) returns %d RowGroups", maxRows, expectedRowGroups);
        assertEquals(msg, expectedRowGroups, rowGroups.size());

        assertRowGroupSizes(rowGroups, maxRows);
    }

    /**
     * Verify that "MaxRows" results in ... a number of RowGroups, each of which contain `maxRows` (or fewer) rows
     */
    @Test
    public void testMaxRows() {
        // if this fails, then the underlying table has changed, and we need to update this test
        assertEquals("InputTable is of expected size", 100_000, testTable.size());

        // should shortcut away
        assertMaxRows(testTable, testTable.size(), 1);
        assertMaxRows(testTable, testTable.size() + 1, 1);

        // should break into 2 groups, both with 50,000 rows
        assertMaxRows(testTable, testTable.size() - 1, 2);

        // this should break down to 9 groups; one with 11,112 rows, and the rest with 11,111
        assertMaxRows(testTable, 11_112, 9);

        // these should both break down to 10 groups, each with 10,000
        assertMaxRows(testTable, 11_111, 10);

        // this should break down to 10 groups, each with 10,000
        assertMaxRows(testTable, 10_000, 10);

        // this should break down to 101 groups; 10 with 991, and the rest with 990
        assertMaxRows(testTable, 999, 101);

        // this should break down to 151 groups; 38 with 662, and the rest with 661
        assertMaxRows(testTable, 666, 151);
    }

    /**
     * A helper method to verify that ... each RowGroup contains only a single value for each of the {@code groupCol}s
     *
     * @param rowGroups the {@link Table} representation of each RowGroup
     * @param groupCol the grouping column(s)
     */
    private static void assertDistinctValues(final @NotNull List<Table> rowGroups, final String[] groupCol) {
        for (int ii = 0; ii < rowGroups.size(); ii++) {
            final String msg = String.format("RowGroup[%d] has unique values for %s", ii, Arrays.toString(groupCol));
            assertEquals(msg, 1, rowGroups.get(ii).selectDistinct(groupCol).size());
        }
    }

    /**
     * Verify that "ByGroup" results in ... a number of RowGroups, each of which contains a single `random_int` value
     */
    @Test
    public void testByGroup() {
        // if this fails, then the underlying table has changed, and we need to update this test
        assertTrue("InputTable contains grouping column(s)", testTable.hasColumns(groupCol));

        final Table sortedTable = testTable.sort(groupCol);

        final List<Table> rowGroups = getRowGroups(sortedTable, RowGroupInfo.byGroup(groupCol));
        final long expectedRowGroups = sortedTable.partitionBy(groupCol).constituents().length;

        final String msgNoMax = String.format("byGroup(%s) groups", Arrays.toString(groupCol));
        assertEquals(msgNoMax, expectedRowGroups, rowGroups.size());

        assertDistinctValues(rowGroups, groupCol);
    }

    /**
     * Verify that "ByGroup (with max)" results in ... a number of RowGroups, each of which contains a single
     * `random_int` value and contain `maxRows` or fewer * rows
     */
    @Test
    public void testByGroupWithMax() {
        // if this fails, then the underlying table has changed, and we need to update this test
        assertTrue("InputTable contains grouping column(s)", testTable.hasColumns(groupCol));

        final Table sortedTable = testTable.sort(groupCol);
        final long maxRows = 10;

        final List<Table> rowGroups = getRowGroups(sortedTable, RowGroupInfo.byGroup(maxRows, groupCol));
        final long minimumRowGroups = sortedTable.partitionBy(groupCol).constituents().length;

        final String msgNoMax = String.format("byGroup(%d, %s) groups", maxRows, Arrays.toString(groupCol));
        assertTrue(msgNoMax, minimumRowGroups <= rowGroups.size());

        assertDistinctValues(rowGroups, groupCol);
        assertRowGroupSizes(rowGroups, maxRows);
    }

    /**
     * Verify that garbage inputs result in garbage outputs (exceptions)
     */
    @Test
    public void testBadParams() {
        final IllegalArgumentException iae0 = assertThrowsExactly(IllegalArgumentException.class,
                () -> getRowGroups(testTable, RowGroupInfo.splitEvenly(0)));
        assertEquals("Cannot define less than 1 RowGroup", iae0.getMessage());

        final IllegalArgumentException iae1 =
                assertThrowsExactly(IllegalArgumentException.class, () -> RowGroupInfo.withMaxRows(0));
        assertEquals("MaxRows must be positive", iae1.getMessage());
    }

    /**
     * Verify exception is thrown when attempting `byGroup(...)` for improperly ordered input-table
     */
    @Test
    public void testMisordered() {
        // if this fails, then the underlying table has changed, and we need to update this test
        assertTrue("InputTable contains grouping column(s)", testTable.hasColumns(groupCol));

        final IllegalStateException iae = assertThrowsExactly(IllegalStateException.class,
                () -> getRowGroups(testTable, RowGroupInfo.byGroup(groupCol)));
        assertTrue("byGroup(...) fail message is informative",
                iae.getMessage()
                        .startsWith(String.format("Misordered for Grouping column(s) %s:", Arrays.toString(groupCol))));

    }
}
