//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.time.DateTimeUtils.epochNanosToInstant;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.deephaven.engine.testutil.StepClock;
import io.deephaven.engine.util.TableTools;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for Sorted and Unsorted ClockFilter implementations.
 */
public class TestClockFilters {

    private Table testInput1;
    private Table testInput2;
    private Table testInput3;

    private StepClock clock;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Before
    public void setUp() {
        final Table testInputRangeA = newTable(
                col("Timestamp", epochNanosToInstant(1000L), epochNanosToInstant(2000L), epochNanosToInstant(3000L),
                        epochNanosToInstant(1000L), epochNanosToInstant(2000L), epochNanosToInstant(3000L)),
                intCol("Int", 1, 2, 3, 1, 2, 3));
        testInput1 = merge(testInputRangeA, testInputRangeA, testInputRangeA);
        final Table testInputRangeB = newTable(
                col("Timestamp", epochNanosToInstant(2000L), epochNanosToInstant(2000L), epochNanosToInstant(3000L),
                        epochNanosToInstant(2000L), epochNanosToInstant(2000L), epochNanosToInstant(3000L)),
                intCol("Int", 2, 2, 3, 2, 2, 3));
        testInput2 = merge(testInputRangeA, testInputRangeB, testInputRangeA);
        testInput3 = merge(testInputRangeA, testInputRangeB, testInputRangeB);
        clock = new StepClock(1000L, 2000L, 3000L);
    }

    @Test
    public void testSorted1() {
        clock.reset();
        final SortedClockFilter filter = new SortedClockFilter("Timestamp", clock, true);

        final Table result = testInput1.sort("Timestamp").where(filter);
        assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2},
                ColumnVectors.ofInt(result, "Int").toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                ColumnVectors.ofInt(result, "Int").toArray());
    }

    @Test
    public void testUnsorted1() {
        clock.reset();
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);

        final Table result = testInput1.where(filter);
        assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2},
                ColumnVectors.ofInt(result, "Int").toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3},
                ColumnVectors.ofInt(result, "Int").toArray());
    }

    @Test
    public void testSorted2() {
        clock.reset();
        final SortedClockFilter filter = new SortedClockFilter("Timestamp", clock, true);

        final Table result = testInput2.sort("Timestamp").where(filter);
        assertArrayEquals(new int[] {1, 1, 1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2},
                ColumnVectors.ofInt(result, "Int").toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                ColumnVectors.ofInt(result, "Int").toArray());
    }

    @Test
    public void testUnsorted2() {
        clock.reset();
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);

        final Table result = testInput2.where(filter);
        assertArrayEquals(new int[] {1, 1, 1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 1, 2, 1, 2},
                ColumnVectors.ofInt(result, "Int").toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 1, 2, 3, 1, 2, 3},
                ColumnVectors.ofInt(result, "Int").toArray());
    }

    @Test
    public void testSorted3() {
        clock.reset();
        final SortedClockFilter filter = new SortedClockFilter("Timestamp", clock, true);

        final Table result = testInput3.sort("Timestamp").where(filter);
        assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                ColumnVectors.ofInt(result, "Int").toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                ColumnVectors.ofInt(result, "Int").toArray());
    }

    @Test
    public void testUnsorted3() {
        clock.reset();
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);

        final Table result = testInput3.where(filter);
        assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                ColumnVectors.ofInt(result, "Int").toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertArrayEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3},
                ColumnVectors.ofInt(result, "Int").toArray());
    }

    @Test
    public void testInitiallyEmptyUnsorted() {
        clock.reset();
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);

        final Table testEmpty = TableTools.newTable(instantCol("Timestamp"), intCol("Int"));

        final Table result = testEmpty.where(filter);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(0, result.size());
    }

    @Test
    public void testInitiallyEmptySorted() {
        clock.reset();
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);

        final Table testEmpty = TableTools.newTable(instantCol("Timestamp"), intCol("Int"));

        final Table result = testEmpty.where(filter);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            clock.run();
            filter.run();
        });
        assertEquals(0, result.size());
    }
}
