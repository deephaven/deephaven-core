/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.ClockTimeProvider;
import io.deephaven.engine.table.impl.RefreshingTableTestCase;
import io.deephaven.util.clock.RealTimeClock;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.select.AutoTuningIncrementalReleaseFilter;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import junit.framework.TestCase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.table.impl.TstUtils.assertTableEquals;

public class TestIncrementalReleaseFilter extends RefreshingTableTestCase {
    public void testSimple() {
        final Table source = TableTools.newTable(TableTools.intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        TableTools.show(source);

        final IncrementalReleaseFilter incrementalReleaseFilter = new IncrementalReleaseFilter(2, 1);
        final Table filtered = source.where(incrementalReleaseFilter);

        TableTools.show(filtered);
        assertEquals(2, filtered.size());

        for (int ii = 0; ii <= 10; ++ii) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::run);

            TableTools.show(filtered);
            assertEquals(Math.min(3 + ii, 10), filtered.size());
        }
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
        while (filtered.size() < source.size()) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::run);
            cycles++;
        }
        assertTableEquals(source, filtered);
        assertTableEquals(flattened, filtered);
        System.out.println("Cycles: " + cycles);
    }

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
        // I just want to see commas in the output
        UpdateGraphProcessor.DEFAULT.setTargetCycleDurationMillis(100);
        final Table source = TableTools.emptyTable(1_000_000);

        final AutoTuningIncrementalReleaseFilter incrementalReleaseFilter =
                new AutoTuningIncrementalReleaseFilter(0, 100, 1.1, true, new ClockTimeProvider(new RealTimeClock()));
        incrementalReleaseFilter.start();
        final Table filtered = source.where(incrementalReleaseFilter);

        final Table updated = UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> filtered.update("I=ii"));

        int steps = 0;

        while (filtered.size() < source.size()) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::run);
            if (steps++ > 100) {
                TestCase.fail("Did not release rows promptly.");
            }
        }

        assertEquals(source.size(), updated.size());
    }

    private int testAutoTuneCycle(int cycleTime) {
        UpdateGraphProcessor.DEFAULT.setTargetCycleDurationMillis(cycleTime);
        final Table source = TableTools.emptyTable(10_000);
        TableTools.show(source);

        final AutoTuningIncrementalReleaseFilter incrementalReleaseFilter =
                new AutoTuningIncrementalReleaseFilter(0, 100, 1.1, true, new ClockTimeProvider(new RealTimeClock()));
        incrementalReleaseFilter.start();
        final Table filtered = source.where(incrementalReleaseFilter);

        final Table updated = UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> filtered
                .update("I=io.deephaven.engine.table.impl.util.TestIncrementalReleaseFilter.sleepValue(100000, ii)"));

        int cycles = 0;
        while (filtered.size() < source.size()) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::run);
            System.out.println(filtered.size() + " / " + updated.size());
            if (cycles++ > (2 * (source.size() * 100) / cycleTime)) {
                TestCase.fail("Did not release rows promptly.");
            }
        }
        return cycles;
    }
}
