package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.impl.RefreshingTableTestCase;

import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.merge;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.time.DateTimeUtils.nanosToTime;
import static io.deephaven.engine.table.impl.TstUtils.StepClock;

/**
 * Test for Sorted and Unsorted ClockFilter implementations.
 */
public class TestClockFilters extends RefreshingTableTestCase {

    private final Table testInput1;
    private final Table testInput2;
    private final Table testInput3;
    {
        final Table testInputRangeA = newTable(
                col("Timestamp", nanosToTime(1000L), nanosToTime(2000L), nanosToTime(3000L), nanosToTime(1000L),
                        nanosToTime(2000L), nanosToTime(3000L)),
                intCol("Int", 1, 2, 3, 1, 2, 3));
        testInput1 = merge(testInputRangeA, testInputRangeA, testInputRangeA);
        final Table testInputRangeB = newTable(
                col("Timestamp", nanosToTime(2000L), nanosToTime(2000L), nanosToTime(3000L), nanosToTime(2000L),
                        nanosToTime(2000L), nanosToTime(3000L)),
                intCol("Int", 2, 2, 3, 2, 2, 3));
        testInput2 = merge(testInputRangeA, testInputRangeB, testInputRangeA);
        testInput3 = merge(testInputRangeA, testInputRangeB, testInputRangeB);
    }

    private final StepClock clock = new StepClock(1000L, 2000L, 3000L);

    public void testSorted1() {
        clock.reset();
        final SortedClockFilter filter = new SortedClockFilter("Timestamp", clock, true);

        final Table result = testInput1.sort("Timestamp").where(filter);
        assertEquals(new int[] {1, 1, 1, 1, 1, 1}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                (int[]) result.getColumn("Int").getDirect());
    }

    public void testUnsorted1() {
        clock.reset();
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);

        final Table result = testInput1.where(filter);
        assertEquals(new int[] {1, 1, 1, 1, 1, 1}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3},
                (int[]) result.getColumn("Int").getDirect());
    }

    public void testSorted2() {
        clock.reset();
        final SortedClockFilter filter = new SortedClockFilter("Timestamp", clock, true);

        final Table result = testInput2.sort("Timestamp").where(filter);
        assertEquals(new int[] {1, 1, 1, 1}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                (int[]) result.getColumn("Int").getDirect());
    }

    public void testUnsorted2() {
        clock.reset();
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);

        final Table result = testInput2.where(filter);
        assertEquals(new int[] {1, 1, 1, 1}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 1, 2, 1, 2}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 1, 2, 3, 1, 2, 3},
                (int[]) result.getColumn("Int").getDirect());
    }

    public void testSorted3() {
        clock.reset();
        final SortedClockFilter filter = new SortedClockFilter("Timestamp", clock, true);

        final Table result = testInput3.sort("Timestamp").where(filter);
        assertEquals(new int[] {1, 1}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                (int[]) result.getColumn("Int").getDirect());
    }

    public void testUnsorted3() {
        clock.reset();
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);

        final Table result = testInput3.where(filter);
        assertEquals(new int[] {1, 1}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2}, (int[]) result.getColumn("Int").getDirect());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3},
                (int[]) result.getColumn("Int").getDirect());
    }
}
