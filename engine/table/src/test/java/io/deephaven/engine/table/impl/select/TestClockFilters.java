//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.filter.Filter;
import io.deephaven.base.Factory;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.time.DateTimeUtils.epochNanosToInstant;
import static org.junit.Assert.*;

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

    @Test
    public void testSortedWithBarriers() {
        Factory<SortedClockFilter> factory = () -> new SortedClockFilter("Timestamp", clock, true);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // clock filter declares barrier, following RowSetCapturingFilters respects it barriers
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter()) {

            clock.reset();

            final SortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1,
                    clockFilter.withDeclaredBarriers("A"),
                    filter2.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(2, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(10, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(6, filter2.numRowsProcessed());
        }

        // Clock filter between two RowSetCapturingFilters with barriers
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter()) {

            clock.reset();

            final SortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1.withDeclaredBarriers("A"),
                    clockFilter,
                    filter2.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(2, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(10, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(6, filter2.numRowsProcessed());
        }

        // Clock filter followed by two RowSetCapturingFilters with barriers
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter()) {

            clock.reset();
            final SortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    clockFilter,
                    filter1.withDeclaredBarriers("A"),
                    filter2.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(2, filter1.numRowsProcessed());
            assertEquals(2, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(10, filter1.numRowsProcessed());
            assertEquals(10, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(6, filter1.numRowsProcessed());
            assertEquals(6, filter2.numRowsProcessed());
        }

        // Two RowSetCapturingFilters with barriers followed by Clock filter
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter()) {

            clock.reset();
            final SortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1.withDeclaredBarriers("A"),
                    filter2.withRespectedBarriers("A"),
                    clockFilter));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(18, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
        }

        // Four RowSetCapturingFilters with barriers surrounding Clock filter
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter3 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter4 = new RowSetCapturingFilter()) {
            clock.reset();
            final SortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1.withDeclaredBarriers("A"),
                    filter2.withDeclaredBarriers("B"),
                    clockFilter,
                    filter3.withRespectedBarriers("B"),
                    filter4.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(18, filter2.numRowsProcessed());
            assertEquals(2, filter3.numRowsProcessed());
            assertEquals(2, filter4.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            filter3.reset();
            filter4.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
            assertEquals(10, filter3.numRowsProcessed());
            assertEquals(10, filter4.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            filter3.reset();
            filter4.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
            assertEquals(6, filter3.numRowsProcessed());
            assertEquals(6, filter4.numRowsProcessed());
        }

        // Four RowSetCapturingFilters with barriers surrounding Clock filter with respected barrier
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter3 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter4 = new RowSetCapturingFilter()) {
            clock.reset();
            final SortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1.withDeclaredBarriers("A"),
                    filter2.withDeclaredBarriers("B"),
                    filter3.withRespectedBarriers("B"),
                    clockFilter.withRespectedBarriers("A"),
                    filter4.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(18, filter2.numRowsProcessed());
            assertEquals(18, filter3.numRowsProcessed());
            assertEquals(2, filter4.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            filter3.reset();
            filter4.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
            assertEquals(0, filter3.numRowsProcessed());
            assertEquals(10, filter4.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            filter3.reset();
            filter4.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
            assertEquals(0, filter3.numRowsProcessed());
            assertEquals(6, filter4.numRowsProcessed());
        }
    }

    @Test
    public void testUnsortedWithBarriers() {
        Factory<UnsortedClockFilter> factory = () -> new UnsortedClockFilter("Timestamp", clock, true);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // clock filter declares barrier, following RowSetCapturingFilters respects it barriers
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter()) {
            clock.reset();
            final UnsortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1,
                    clockFilter.withDeclaredBarriers("A"),
                    filter2.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(2, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(10, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(6, filter2.numRowsProcessed());
        }

        // Sandwiched clock filter between two RowSetCapturingFilters with barriers
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter()) {
            clock.reset();
            final UnsortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1.withDeclaredBarriers("A"),
                    clockFilter,
                    filter2.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(2, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(10, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(6, filter2.numRowsProcessed());
        }

        // Clock filter followed by two RowSetCapturingFilters with barriers
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter()) {
            clock.reset();
            final UnsortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    clockFilter,
                    filter1.withDeclaredBarriers("A"),
                    filter2.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(2, filter1.numRowsProcessed());
            assertEquals(2, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(10, filter1.numRowsProcessed());
            assertEquals(10, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(6, filter1.numRowsProcessed());
            assertEquals(6, filter2.numRowsProcessed());
        }

        // Two RowSetCapturingFilters with barriers followed by Clock filter
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter()) {
            clock.reset();
            final UnsortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1.withDeclaredBarriers("A"),
                    filter2.withRespectedBarriers("A"),
                    clockFilter));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(18, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
        }

        // Four RowSetCapturingFilters with barriers surrounding Clock filter
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter3 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter4 = new RowSetCapturingFilter()) {
            clock.reset();
            final UnsortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1.withDeclaredBarriers("A"),
                    filter2.withDeclaredBarriers("B"),
                    clockFilter,
                    filter3.withRespectedBarriers("B"),
                    filter4.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(18, filter2.numRowsProcessed());
            assertEquals(2, filter3.numRowsProcessed());
            assertEquals(2, filter4.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            filter3.reset();
            filter4.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
            assertEquals(10, filter3.numRowsProcessed());
            assertEquals(10, filter4.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            filter3.reset();
            filter4.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
            assertEquals(6, filter3.numRowsProcessed());
            assertEquals(6, filter4.numRowsProcessed());
        }

        // Four RowSetCapturingFilters with barriers surrounding Clock filter with respected barrier
        try (final RowSetCapturingFilter filter1 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter2 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter3 = new RowSetCapturingFilter();
                final RowSetCapturingFilter filter4 = new RowSetCapturingFilter()) {
            clock.reset();
            final UnsortedClockFilter clockFilter = factory.create();
            final Table result = testInput3.where(Filter.and(
                    filter1.withDeclaredBarriers("A"),
                    filter2.withDeclaredBarriers("B"),
                    filter3.withRespectedBarriers("B"),
                    clockFilter.withRespectedBarriers("A"),
                    filter4.withRespectedBarriers("A")));
            assertArrayEquals(new int[] {1, 1}, ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(18, filter1.numRowsProcessed());
            assertEquals(18, filter2.numRowsProcessed());
            assertEquals(18, filter3.numRowsProcessed());
            assertEquals(2, filter4.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            filter3.reset();
            filter4.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
            assertEquals(0, filter3.numRowsProcessed());
            assertEquals(10, filter4.numRowsProcessed());

            filter1.reset();
            filter2.reset();
            filter3.reset();
            filter4.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                clockFilter.run();
            });
            assertArrayEquals(new int[] {1, 2, 3, 1, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2, 3},
                    ColumnVectors.ofInt(result, "Int").toArray());

            assertEquals(0, filter1.numRowsProcessed());
            assertEquals(0, filter2.numRowsProcessed());
            assertEquals(0, filter3.numRowsProcessed());
            assertEquals(6, filter4.numRowsProcessed());
        }
    }

    /**
     * Reindexing filters will throw {@link UnsupportedOperationException} when added to composed filters or when
     * inverted. This test codifies that behavior to signal additional changes will bve required if this changes.
     */
    @Test
    public void verifyFilterExclusions() {
        final UnsortedClockFilter unsortedFilter = new UnsortedClockFilter("Timestamp", clock, true);
        assertTrue("unsortedFilter instanceof ReindexingFilter", unsortedFilter instanceof ReindexingFilter);

        try {
            final WhereFilter filter = ConjunctiveFilter.makeConjunctiveFilter(unsortedFilter, unsortedFilter);
            fail("Expected UnsupportedOperationException when composing reindexing filter");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            final WhereFilter filter = DisjunctiveFilter.makeDisjunctiveFilter(unsortedFilter, unsortedFilter);
            fail("Expected UnsupportedOperationException when composing reindexing filter");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            final WhereFilter filter = (WhereFilter) unsortedFilter.invert();
            fail("Expected UnsupportedOperationException when inverting reindexing filter");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }
}
