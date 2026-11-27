//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.SortSpec;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.compare.ByteComparisons;
import io.deephaven.util.datastructures.SizeException;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.show;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(OutOfBandTest.class)
public class QueryTableHugeSortTest {

    @Rule
    public final EngineCleanup rule = new EngineCleanup();

    @Test
    public void testHugeSort() {
        final int megaSortSize = SortHelpers.megaSortSize;
        final int sortChunkSize = SortHelpers.sortChunkSize;
        try {
            // ideally we would sort something that is bigger than Integer.MAX_VALUE, but the test VMs can not handle
            // that.
            // So instead we adjust the mega sort parameters so that we'll exercise the code path anyway.
            SortHelpers.megaSortSize = 1 << 24; // 16 Million
            SortHelpers.sortChunkSize = 1 << 21; // 2 Million

            final long tableSize = (long) SortHelpers.megaSortSize * 2L;
            final Table bigTable = TableTools.emptyTable(tableSize).updateView("SortCol=(byte)(ii%100)", "Sentinel=k");
            TableTools.show(bigTable);

            final long runSize1 = (tableSize + 99) / 100;
            final long runSize2 = runSize1 - 1;
            final long firstSmallRun = tableSize % 100;
            final long runSizePivot = (firstSmallRun) * runSize1;
            System.out.println("RunSize: " + runSize1 + ", " + runSize2 + ", pivot: " + runSizePivot
                    + ", firstSmallRun: " + firstSmallRun);

            final long startTime = System.currentTimeMillis();
            final Table sorted = bigTable.sort("SortCol");
            final long duration = System.currentTimeMillis() - startTime;

            System.out.println("Duration " + duration + "ms");

            TableTools.showWithRowSet(sorted);

            QueryScope.addParam("runSize1", runSize1);
            QueryScope.addParam("runSize2", runSize2);
            QueryScope.addParam("runSizePivot", runSizePivot);
            QueryScope.addParam("firstSmallRun", firstSmallRun);

            final Table expected = TableTools.emptyTable(tableSize).updateView(
                    "SortCol=(byte)(ii < runSizePivot ? ii/runSize1 : ((ii - runSizePivot) / runSize2) + firstSmallRun)",
                    "Sentinel=(ii < runSizePivot) ? ((100 * (ii % runSize1)) + SortCol) : 100 * ((ii - runSizePivot) % runSize2) + SortCol");
            TableTools.showWithRowSet(expected);

            assertTableEquals(expected, sorted);
        } finally {
            SortHelpers.megaSortSize = megaSortSize;
            SortHelpers.sortChunkSize = sortChunkSize;
        }
    }

    /**
     * A ticking sort of at least megaSortSize rows (lowered here) fails with a message that states the number of rows
     * and the limit.
     */
    @Test
    public void testRefreshingSortAtMegaSortSize() {
        final QueryTable table = TstUtils.testRefreshingTable(TableTools.intCol("A", 5, 3, 1, 4, 2, 0));
        checkMegaSortSizeLimit(table, "Cannot sort a ticking table: Input size 6 larger than maximum 3");
    }

    /**
     * A ticking sort that grows to megaSortSize rows (lowered here) fails with a message that states the number of rows
     * and the limit.
     */
    @Test
    public void testRefreshingSortGrowingToMegaSortSize() {
        final int megaSortSize = SortHelpers.megaSortSize;
        final int sortChunkSize = SortHelpers.sortChunkSize;
        try {
            SortHelpers.megaSortSize = 4;
            SortHelpers.sortChunkSize = 2;
            final QueryTable source = TstUtils.testRefreshingTable(TableTools.intCol("A", 5, 3, 1));
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            final Table sorted = updateGraph.sharedLock().computeLocked(() -> source.sort("A"));
            rule.allowingError(() -> updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, RowSetFactory.fromKeys(3), TableTools.intCol("A", 4));
                source.notifyListeners(RowSetFactory.fromKeys(3), RowSetFactory.empty(), RowSetFactory.empty());
            }), errors -> errors.size() == 1 && errors.get(0) instanceof SizeException
                    && "Cannot sort a ticking table: Input size 4 larger than maximum 3"
                            .equals(errors.get(0).getMessage()));
            assertTrue(sorted.isFailed());
        } finally {
            SortHelpers.megaSortSize = megaSortSize;
            SortHelpers.sortChunkSize = sortChunkSize;
        }
    }

    /**
     * A blink table sort maps a cycle of at least megaSortSize rows (lowered here) with the mega sort's column source
     * of row keys and a smaller cycle with an array of them, in any order of cycles, sorting by one column and by two.
     */
    @Test
    public void testBlinkSortAtMegaSortSize() {
        final int megaSortSize = SortHelpers.megaSortSize;
        final int sortChunkSize = SortHelpers.sortChunkSize;
        try {
            SortHelpers.megaSortSize = 4;
            SortHelpers.sortChunkSize = 2;
            checkBlinkSortCycles("A");
            checkBlinkSortCycles("A", "B");
        } finally {
            SortHelpers.megaSortSize = megaSortSize;
            SortHelpers.sortChunkSize = sortChunkSize;
        }
    }

    /**
     * Sort a blink table by sortColumns through cycles of several sizes, at least and below megaSortSize, and check
     * each cycle's rows are its rows sorted by A, then B when sorting by both, then row key.
     */
    private static void checkBlinkSortCycles(final String... sortColumns) {
        final Random random = new Random(sortColumns.length);
        final int[] cycleSizes = {6, 2, 5, 3, 7, 1, 4};
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final int[][] firstCycle = randomBlinkCycle(random, cycleSizes[0]);
        final QueryTable source = TstUtils.testRefreshingTable(RowSetFactory.flat(cycleSizes[0]).toTracking(),
                TableTools.intCol("A", firstCycle[0]), TableTools.intCol("B", firstCycle[1]));
        source.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
        final Table sorted = updateGraph.sharedLock().computeLocked(() -> source.sort(sortColumns));
        assertTableEquals(expectedBlinkSort(firstCycle, sortColumns.length), sorted);

        long firstKey = 0;
        for (int ci = 1; ci < cycleSizes.length; ++ci) {
            final int[][] cycle = randomBlinkCycle(random, cycleSizes[ci]);
            final long removedFirstKey = firstKey;
            final long removedLastKey = firstKey + cycleSizes[ci - 1] - 1;
            firstKey = removedLastKey + 1;
            final long addedFirstKey = firstKey;
            final long addedLastKey = firstKey + cycleSizes[ci] - 1;
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(source, RowSetFactory.fromRange(removedFirstKey, removedLastKey));
                TstUtils.addToTable(source, RowSetFactory.fromRange(addedFirstKey, addedLastKey),
                        TableTools.intCol("A", cycle[0]), TableTools.intCol("B", cycle[1]));
                source.notifyListeners(RowSetFactory.fromRange(addedFirstKey, addedLastKey),
                        RowSetFactory.fromRange(removedFirstKey, removedLastKey), RowSetFactory.empty());
            });
            assertTableEquals(expectedBlinkSort(cycle, sortColumns.length), sorted);
        }
    }

    /** Columns A and B of a blink cycle of size rows, with values from 0 to 3 so that many rows tie. */
    private static int[][] randomBlinkCycle(final Random random, final int size) {
        final int[][] cycle = new int[2][size];
        for (int ii = 0; ii < size; ++ii) {
            cycle[0][ii] = random.nextInt(4);
            cycle[1][ii] = random.nextInt(4);
        }
        return cycle;
    }

    /**
     * The rows of a blink cycle, in row key order, sorted by A, then by B when sortColumnCount is two, keeping tied
     * rows in row key order.
     */
    private static Table expectedBlinkSort(final int[][] cycle, final int sortColumnCount) {
        final List<Integer> order = new ArrayList<>();
        for (int ii = 0; ii < cycle[0].length; ++ii) {
            order.add(ii);
        }
        final Comparator<Integer> byA = Comparator.comparingInt(row -> cycle[0][row]);
        order.sort(sortColumnCount == 1 ? byA : byA.thenComparingInt(row -> cycle[1][row]));
        return TableTools.newTable(TableTools.intCol("A", order.stream().mapToInt(row -> cycle[0][row]).toArray()),
                TableTools.intCol("B", order.stream().mapToInt(row -> cycle[1][row]).toArray()));
    }

    private static void checkMegaSortSizeLimit(final QueryTable table, final String expectedMessage) {
        final int megaSortSize = SortHelpers.megaSortSize;
        final int sortChunkSize = SortHelpers.sortChunkSize;
        try {
            SortHelpers.megaSortSize = 4;
            SortHelpers.sortChunkSize = 2;
            final SizeException exception = org.junit.Assert.assertThrows(SizeException.class, () -> table.sort("A"));
            assertEquals(expectedMessage, exception.getMessage());
            assertEquals(3, exception.getMaximumSize());
        } finally {
            SortHelpers.megaSortSize = megaSortSize;
            SortHelpers.sortChunkSize = sortChunkSize;
        }
    }

    @Test
    public void testHugeIndexedSort() {
        final String[] captains = new String[] {"Hornigold", "Jennings", "Vane", "Bellamy"};

        final long tableSize = 1L << 24; // 16 MM (note we msut be a multiple of captains.length)
        final long segSize = tableSize / captains.length;

        QueryScope.addParam("captains", captains);
        QueryScope.addParam("segSize", segSize);
        final Table indexed =
                TableTools.emptyTable(tableSize).updateView("Captain=captains[(int)(ii / segSize)]", "Sentinel=ii");

        final ColumnSource<Object> captainSource = (ColumnSource<Object>) indexed.getColumnSource("Captain");

        // Create the index for this table and column.
        DataIndexer.getOrCreateDataIndex(indexed, "Captain");

        final long sortStart = System.currentTimeMillis();
        final Table sortedIndexed = indexed.sortDescending("Captain");
        final long sortDuration = System.currentTimeMillis() - sortStart;
        System.out.println("Sort Duration: " + sortDuration + "ms");

        show(sortedIndexed);

        final String[] sortedCaptains = Arrays.copyOf(captains, captains.length);
        Arrays.sort(sortedCaptains, Comparator.reverseOrder());
        QueryScope.addParam("sortedCaptains", sortedCaptains);
        final Table sortedValues = TableTools.emptyTable(tableSize)
                .updateView("Captain=sortedCaptains[(int)(ii / segSize)]", "Sentinel=ii");

        System.out.println("Comparing tables:");
        final long compareStart = System.currentTimeMillis();
        assertTableEquals(sortedValues.view("Captain"), sortedIndexed.view("Captain"));
        final long compareDuration = System.currentTimeMillis() - compareStart;
        System.out.println("Compare Duration: " + compareDuration + "ms");
    }

    static class NumericStringComparator implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            if (o1 == o2) {
                return 0;
            }
            if (o1 == null) {
                return -1;
            }
            if (o2 == null) {
                return 1;
            }
            final byte b1 = Byte.parseByte(o1);
            final byte b2 = Byte.parseByte(o2);
            return ByteComparisons.compare(b1, b2);
        }
    }

    @Test
    public void testNoComparators() {
        final int megaSortSize = SortHelpers.megaSortSize;
        final int sortChunkSize = SortHelpers.sortChunkSize;
        try {
            // ideally we would sort something that is bigger than Integer.MAX_VALUE, but the test VMs can not handle
            // that.
            // So instead we adjust the mega sort parameters so that we'll exercise the code path anyway.
            SortHelpers.megaSortSize = 1 << 24; // 16 Million
            SortHelpers.sortChunkSize = 1 << 21; // 2 Million

            final long bigSize = (long) SortHelpers.megaSortSize * 2L;
            final int smallSize = SortHelpers.megaSortSize / 64;
            final Table smallTable =
                    TableTools.emptyTable(smallSize).updateView("SortCol=Byte.toString((byte)(ii%100))", "Sentinel=k");

            final SortSpec sortBy = ComparatorSortColumn.asc("SortCol", new NumericStringComparator(), true);

            final Table sorted = ((QueryTable) smallTable).sort(sortBy);

            final long runSize1 = (smallSize + 99) / 100;
            final long runSize2 = runSize1 - 1;
            final long firstSmallRun = smallSize % 100;
            final long runSizePivot = (firstSmallRun) * runSize1;

            QueryScope.addParam("runSize1", runSize1);
            QueryScope.addParam("runSize2", runSize2);
            QueryScope.addParam("runSizePivot", runSizePivot);
            QueryScope.addParam("firstSmallRun", firstSmallRun);

            final Table expected = TableTools.emptyTable(smallSize).updateView(
                    "SortCol=Byte.toString((byte)(ii < runSizePivot ? ii/runSize1 : ((ii - runSizePivot) / runSize2) + firstSmallRun))",
                    "Sentinel=(ii < runSizePivot) ? ((100 * (ii % runSize1)) + Byte.parseByte(SortCol)) : 100 * ((ii - runSizePivot) % runSize2) + Byte.parseByte(SortCol)");

            assertTableEquals(expected, sorted);

            final Table bigTable =
                    TableTools.emptyTable(bigSize).updateView("SortCol=Byte.toString((byte)(ii%100))", "Sentinel=k");

            final UnsupportedOperationException iae2 =
                    org.junit.Assert.assertThrows(UnsupportedOperationException.class,
                            () -> ((QueryTable) bigTable).sort(sortBy));
            assertEquals("Cannot sort more than " + SortHelpers.megaSortSize + " rows with a comparator",
                    iae2.getMessage());

        } finally {
            SortHelpers.megaSortSize = megaSortSize;
            SortHelpers.sortChunkSize = sortChunkSize;
        }
    }
}
