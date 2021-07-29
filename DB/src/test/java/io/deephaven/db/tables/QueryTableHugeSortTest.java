package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.show;
import static io.deephaven.db.v2.TstUtils.assertTableEquals;

@Category(OutOfBandTest.class)
public class QueryTableHugeSortTest {
    @Test
    public void testHugeSort() {
        final int megaSortSize = SortHelpers.megaSortSize;
        final int sortChunkSize = SortHelpers.sortChunkSize;
        try {
            // ideally we would sort something that is bigger than Integer.MAX_VALUE, but the test VMs can not handle that.
            // So instead we adjust the mega sort parameters so that we'll exercise the code path anyway.
            SortHelpers.megaSortSize = 1 << 24; // 16 Million
            SortHelpers.sortChunkSize = 1 << 21; // 2 Million

            final long tableSize = (long)SortHelpers.megaSortSize * 2L;
            final Table bigTable = TableTools.emptyTable(tableSize).updateView("SortCol=(byte)(ii%100)", "Sentinel=k");
            TableTools.show(bigTable);

            final long runSize1 = (tableSize + 99) / 100;
            final long runSize2 = runSize1 - 1;
            final long firstSmallRun = tableSize % 100;
            final long runSizePivot = (firstSmallRun) * runSize1;
            System.out.println("RunSize: " + runSize1 + ", " + runSize2 + ", pivot: " + runSizePivot + ", firstSmallRun: " + firstSmallRun);

            final long startTime = System.currentTimeMillis();
            final Table sorted = bigTable.sort("SortCol");
            final long duration = System.currentTimeMillis() - startTime;

            System.out.println("Duration " + duration + "ms");

            TableTools.showWithIndex(sorted);

            QueryScope.addParam("runSize1", runSize1);
            QueryScope.addParam("runSize2", runSize2);
            QueryScope.addParam("runSizePivot", runSizePivot);
            QueryScope.addParam("firstSmallRun", firstSmallRun);

            final Table expected = TableTools.emptyTable(tableSize).updateView("SortCol=(byte)(ii < runSizePivot ? ii/runSize1 : ((ii - runSizePivot) / runSize2) + firstSmallRun)", "Sentinel=(ii < runSizePivot) ? ((100 * (ii % runSize1)) + SortCol) : 100 * ((ii - runSizePivot) % runSize2) + SortCol");
            TableTools.showWithIndex(expected);

            TstUtils.assertTableEquals(expected, sorted);
        } finally {
            SortHelpers.megaSortSize = megaSortSize;
            SortHelpers.sortChunkSize = sortChunkSize;
        }
    }

    @Test
    public void testHugeGroupedSort() {
        final String [] captains = new String[]{"Hornigold", "Jennings", "Vane", "Bellamy"};

        final long tableSize = 1L<<24; // 16 MM (note we msut be a multiple of captains.length)
        final long segSize = tableSize / captains.length;

        QueryScope.addParam("captains", captains);
        QueryScope.addParam("segSize", segSize);
        final Table grouped = TableTools.emptyTable(tableSize).updateView("Captain=captains[(int)(ii / segSize)]", "Sentinel=ii");
        final Map<String, Index> gtr = new LinkedHashMap<>();
        for (int ii = 0; ii < captains.length; ++ii) {
            gtr.put(captains[ii], Index.FACTORY.getIndexByRange(ii * segSize, (ii + 1) * segSize - 1));
        }
        System.out.println(gtr);
        ((AbstractColumnSource)(grouped.getColumnSource("Captain"))).setGroupToRange(gtr);

        final long sortStart = System.currentTimeMillis();
        final Table sortedGrouped = grouped.sortDescending("Captain");
        final long sortDuration = System.currentTimeMillis() - sortStart;
        System.out.println("Sort Duration: " + sortDuration + "ms");

        show(sortedGrouped);

        final String [] sortedCaptains = Arrays.copyOf(captains, captains.length);
        Arrays.sort(sortedCaptains, Comparator.reverseOrder());
        QueryScope.addParam("sortedCaptains", sortedCaptains);
        final Table sortedValues = TableTools.emptyTable(tableSize).updateView("Captain=sortedCaptains[(int)(ii / segSize)]", "Sentinel=ii");

        System.out.println("Comparing tables:");
        final long compareStart = System.currentTimeMillis();
        assertTableEquals(sortedValues.view("Captain"), sortedGrouped.view("Captain"));
        final long compareDuration = System.currentTimeMillis() - compareStart;
        System.out.println("Compare Duration: " + compareDuration + "ms");
    }

    @After
    public void clearScope() {
        QueryScope.setScope(new QueryScope.StandaloneImpl());
    }
}
