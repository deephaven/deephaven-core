//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.select.AutoTuningIncrementalReleaseFilter;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import junit.framework.TestCase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;

public class TestIncrementalReleaseFilter extends RefreshingTableTestCase {
    public void testSimple() {
        final Table source = TableTools.newTable(TableTools.intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        TableTools.show(source);

        final IncrementalReleaseFilter incrementalReleaseFilter = new IncrementalReleaseFilter(2, 1);
        final Table filtered = source.where(incrementalReleaseFilter);

        TableTools.show(filtered);
        assertEquals(2, filtered.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int ii = 0; ii <= 10; ++ii) {
            updateGraph.runWithinUnitTestCycle(incrementalReleaseFilter::run);

            TableTools.show(filtered);
            assertEquals(Math.min(3 + ii, 10), filtered.size());
            if (filtered.size() == source.size()) {
                break;
            }
        }
        assertEquals(source.size(), filtered.size());
    }

    public void testBigTable() {
        final Table sourcePart = TableTools.emptyTable(1_000_000_000L);
        final List<Table> sourceParts = IntStream.range(0, 20).mapToObj(x -> sourcePart).collect(Collectors.toList());
        final Table source = TableTools.merge(sourceParts);
        TableTools.show(source);

        final IncrementalReleaseFilter incrementalReleaseFilter = new IncrementalReleaseFilter(2, 10_000_000);
        final Table filtered = source.where(incrementalReleaseFilter);
        final Table flattened = filtered.flatten();

        assertEquals(2, filtered.size());

        int cycles = 0;
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        while (filtered.size() < source.size()) {
            updateGraph.runWithinUnitTestCycle(incrementalReleaseFilter::run);
            cycles++;
        }
        assertTableEquals(source, filtered);
        assertTableEquals(flattened, filtered);
        System.out.println("Cycles: " + cycles);
    }

    @SuppressWarnings("unused") // used by testAutoTuneCycle via an update query
    static public <T> T sleepValue(long duration, T retVal) {
        final Object blech = new Object();
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (blech) {
            try {
                final long seconds = duration / 1000000000L;
                final int nanos = (int) (duration % 1000000000L);
                blech.wait(seconds, nanos);
            } catch (InterruptedException e) {
            }
        }
        return retVal;
    }

    public void testAutoTune() {
        final int cycles50 = testAutoTuneCycle(50);
        final int cycles100 = testAutoTuneCycle(100);
        final int cycles1000 = testAutoTuneCycle(1000);
        System.out.println("50ms: " + cycles50);
        System.out.println("100ms: " + cycles100);
        System.out.println("1000ms: " + cycles1000);
    }

    public void testAutoTune2() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // I just want to see commas in the output
        updateGraph.setTargetCycleDurationMillis(100);
        final Table source = TableTools.emptyTable(1_000_000);

        final AutoTuningIncrementalReleaseFilter incrementalReleaseFilter =
                new AutoTuningIncrementalReleaseFilter(0, 100, 1.1, true);
        incrementalReleaseFilter.start();
        final Table filtered = source.where(incrementalReleaseFilter);

        final Table updated = updateGraph.sharedLock().computeLocked(() -> filtered.update("I=0"));

        int steps = 0;

        while (filtered.size() < source.size()) {
            updateGraph.runWithinUnitTestCycle(incrementalReleaseFilter::run);
            if (steps++ > 100) {
                TestCase.fail("Did not release rows promptly.");
            }
        }

        assertEquals(source.size(), updated.size());
    }

    private int testAutoTuneCycle(int cycleTime) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.setTargetCycleDurationMillis(cycleTime);
        final Table source = TableTools.emptyTable(10_000);
        TableTools.show(source);

        final AutoTuningIncrementalReleaseFilter incrementalReleaseFilter =
                new AutoTuningIncrementalReleaseFilter(0, 100, 1.1, true);
        incrementalReleaseFilter.start();
        final Table filtered = source.updateView("I = ii").where(incrementalReleaseFilter);

        final Table updated = updateGraph.sharedLock().computeLocked(() -> filtered
                .update("I=io.deephaven.engine.table.impl.util.TestIncrementalReleaseFilter.sleepValue(100000, I)"));

        int cycles = 0;
        while (filtered.size() < source.size()) {
            updateGraph.runWithinUnitTestCycle(incrementalReleaseFilter::run);
            System.out.println(filtered.size() + " / " + updated.size());
            if (cycles++ > (2 * (source.size() * 100) / cycleTime)) {
                TestCase.fail("Did not release rows promptly.");
            }
        }
        return cycles;
    }
}
