//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.TstUtils;

public class TestIncrementalReleaseFilter extends RefreshingTableTestCase {
    public void testSimple() {
        final Table source = TableTools.newTable(TableTools.intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        final IncrementalReleaseFilter incrementalReleaseFilter = new IncrementalReleaseFilter(2, 1);
        final Table filtered = source.where(incrementalReleaseFilter);

        assertEquals(2, filtered.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int ii = 0; ii <= 10; ++ii) {
            updateGraph.runWithinUnitTestCycle(incrementalReleaseFilter::run);

            assertEquals(Math.min(3 + ii, 10), filtered.size());
            if (filtered.size() == source.size()) {
                break;
            }
        }
        assertEquals(source.size(), filtered.size());
    }

    public void testSimpleRefreshingAppend() {
        final Table source =
                TstUtils.testRefreshingTable(intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).assertAppendOnly();

        final IncrementalReleaseFilter incrementalReleaseFilter = new IncrementalReleaseFilter(2, 1);
        final Table filtered = source.where(incrementalReleaseFilter);

        assertEquals(2, filtered.size());

        long lastSize = 2;

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step <= 25; ++step) {
            final int iteration = step;
            updateGraph.runWithinUnitTestCycle(() -> {
                incrementalReleaseFilter.run();

                if (iteration == 6) {
                    TstUtils.addToTable(source, i(10, 11, 12, 13, 14), intCol("Sentinel", 11, 12, 13, 14, 15));
                    ((QueryTable) source).notifyListeners(i(10, 11, 12, 13, 14), i(), i());
                }

                if (iteration == 15) {
                    assertEquals(filtered.size(), source.size());
                    TstUtils.addToTable(source, i(15, 16, 17, 18, 19), intCol("Sentinel", 16, 17, 18, 19, 20));
                    ((QueryTable) source).notifyListeners(i(15, 16, 17, 18, 19), i(), i());
                }

                if (iteration == 16) {
                    TstUtils.addToTable(source, i(20, 21), intCol("Sentinel", 21, 22));
                    ((QueryTable) source).notifyListeners(i(20, 21), i(), i());
                }

                if (iteration == 23) {
                    TstUtils.addToTable(source, i(22, 23), intCol("Sentinel", 23, 24));
                    ((QueryTable) source).notifyListeners(i(22, 23), i(), i());
                }
            });

            System.out.println("Step = " + step + ", source=" + source.size() + ", filtered=" + filtered.size());
            final long expectedSize = Math.min(lastSize + 1, source.size());
            assertEquals(expectedSize, filtered.size());
            lastSize = filtered.size();
        }
        assertEquals(source.size(), filtered.size());
    }

    public void testSimpleRefreshingAddOnly() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(0, 1, 2, 3, 4, 10, 11, 12, 13, 14).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 11, 12, 13, 14, 15));

        final IncrementalReleaseFilter incrementalReleaseFilter = new IncrementalReleaseFilter(2, 1);
        final Table filtered = source.where(incrementalReleaseFilter);

        assertEquals(2, filtered.size());

        long lastSize = 2;

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step <= 25; ++step) {
            final int iteration = step;
            updateGraph.runWithinUnitTestCycle(() -> {
                incrementalReleaseFilter.run();

                if (iteration == 6) {
                    TstUtils.addToTable(source, i(5, 6, 7, 15, 16), intCol("Sentinel", 6, 7, 8, 16, 17));
                    source.notifyListeners(i(5, 6, 7, 15, 16), i(), i());
                }

                if (iteration == 15) {
                    assertEquals(filtered.size(), source.size());
                    TstUtils.addToTable(source, i(17, 18, 19, 20, 21), intCol("Sentinel", 18, 19, 20, 21, 22));
                    source.notifyListeners(i(17, 18, 19, 20, 21), i(), i());
                }

                if (iteration == 23) {
                    TstUtils.addToTable(source, i(8, 25), intCol("Sentinel", 9, 26));
                    source.notifyListeners(i(8, 25), i(), i());
                }
            });

            System.out.println("Step = " + step + ", source=" + source.size() + ", filtered=" + filtered.size());
            final long expectedSize = Math.min(lastSize + 1, source.size());
            assertEquals(expectedSize, filtered.size());
            lastSize = filtered.size();
        }
        assertEquals(source.size(), filtered.size());
    }

    public void testBigTable() {
        final Table sourcePart = TableTools.emptyTable(1_000_000_000L);
        final List<Table> sourceParts = IntStream.range(0, 20).mapToObj(x -> sourcePart).collect(Collectors.toList());
        final Table source = TableTools.merge(sourceParts);

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

    public void testWaitForCompletion() throws InterruptedException {
        final Table source = TableTools.emptyTable(1000).update("Sentinel=ii");

        final IncrementalReleaseFilter incrementalReleaseFilter = new IncrementalReleaseFilter(10, 1);
        final Table filtered = source.where(incrementalReleaseFilter);

        assertEquals(10, filtered.size());

        final CountDownLatch latch = new CountDownLatch(1);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        new Thread(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
            }
            while (filtered.size() < source.size()) {
                updateGraph.runWithinUnitTestCycle(incrementalReleaseFilter::run);
            }
        }).start();

        if (source.size() == filtered.size()) {
            TestCase.fail("Released rows before expected.");
        }

        long start1 = System.currentTimeMillis();
        incrementalReleaseFilter.waitForCompletion(100);
        long end1 = System.currentTimeMillis();
        if (end1 - start1 < 100) {
            TestCase.fail("Did not wait long enough.");
        }

        latch.countDown();

        long start2 = System.currentTimeMillis();
        incrementalReleaseFilter.waitForCompletion(10000);
        long end2 = System.currentTimeMillis();
        assertEquals(source.size(), filtered.size());
        System.out.println("Waited " + (end2 - start2) + " ms");

        // test the path where we don't wait at all
        incrementalReleaseFilter.waitForCompletion();
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
