//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.show;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;

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
}
