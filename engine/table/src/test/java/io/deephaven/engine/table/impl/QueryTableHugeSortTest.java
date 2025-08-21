//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.SortSpec;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.compare.ByteComparisons;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.show;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertEquals;

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
