//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.ClockFilter;
import io.deephaven.engine.table.impl.select.SortedClockFilter;
import io.deephaven.engine.table.impl.select.UnsortedClockFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.StepClock;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test for Sorted and Unsorted ClockFilter implementations.
 */
@Category(OutOfBandTest.class)
public class TestClockFilterParallelism {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    /**
     * Helper class to capture, set, and restore parallel where settings.
     */
    private static class ParallelWhereSettings implements SafeCloseable {
        private final boolean oldForce;
        private final boolean oldDisable;
        private final int oldSegments;

        ParallelWhereSettings(boolean force, boolean disable, int segments) {
            this.oldForce = QueryTable.FORCE_PARALLEL_WHERE;
            this.oldDisable = QueryTable.DISABLE_PARALLEL_WHERE;
            this.oldSegments = QueryTable.PARALLEL_WHERE_SEGMENTS;
            QueryTable.FORCE_PARALLEL_WHERE = force;
            QueryTable.DISABLE_PARALLEL_WHERE = disable;
            QueryTable.PARALLEL_WHERE_SEGMENTS = segments;
        }

        @Override
        public void close() {
            QueryTable.FORCE_PARALLEL_WHERE = oldForce;
            QueryTable.DISABLE_PARALLEL_WHERE = oldDisable;
            QueryTable.PARALLEL_WHERE_SEGMENTS = oldSegments;
        }
    }

    private void testClockFilterParallelism(boolean sorted, final int segments) {
        try (final ParallelWhereSettings ignored = new ParallelWhereSettings(true, false, segments)) {
            final String[] times = new String[] {"2026-01-20T09:40:00 America/New_York",
                    "2026-01-20T09:45:00 America/New_York", "2026-01-20T09:50:00 America/New_York",
                    "2026-01-20T10:00:00 America/New_York", "2026-01-20T11:00:00 America/New_York",
                    "2026-01-20T12:00:00 America/New_York", "2026-01-20T13:00:00 America/New_York"};
            final StepClock clock = new StepClock(Arrays.stream(times)
                    .mapToLong(ts -> DateTimeUtils.epochNanos(DateTimeUtils.parseInstant(ts))).toArray());

            final ClockFilter filter;
            if (sorted) {
                filter = new SortedClockFilter("Timestamp", clock, true);
            } else {
                filter = new UnsortedClockFilter("Timestamp", clock, true);
            }

            final Table input =
                    TableTools.emptyTable(10_000_000).updateView("StartTime='2026-01-20T09:30:00 America/New_York'",
                            "Timestamp = StartTime + ii * 'PT0.001s'",
                            "Sentinel=ii");

            final Table result = input.where(filter);

            assertTableEquals(
                    input.where("Timestamp <= '" + DateTimeUtils.epochNanosToInstant(clock.currentTimeNanos()) + "'"),
                    result);
            assertEquals(1 + 600 * 1000, result.size());

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.run();
                filter.run();
            });
            assertTableEquals(
                    input.where("Timestamp <= '" + DateTimeUtils.epochNanosToInstant(clock.currentTimeNanos()) + "'"),
                    result);
            assertEquals(1 + 900 * 1000, result.size());

            for (int step = 2; step < times.length; step++) {
                updateGraph.runWithinUnitTestCycle(() -> {
                    clock.run();
                    filter.run();
                });
            }
            assertTableEquals(input, result);
        }
    }

    @Test
    public void testUnsortedParallelism() {
        testClockFilterParallelism(false, 16);
    }

    @Test
    public void testSortedParallelism() {
        final ExecutionContext currentContext = ExecutionContext.getContext();
        final ExecutionContext newContext = currentContext.withOperationInitializer(
                new RandomizedOperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, 20));

        try (final SafeCloseable ignored = newContext.open()) {
            for (int segments = 2; segments <= 16; segments *= 2) {
                testClockFilterParallelism(true, segments);
            }
        }
    }
}
