/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.clock.RealTimeClock;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.LiveTableTestCase;
import io.deephaven.db.v2.select.AutoTuningIncrementalReleaseFilter;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.db.v2.TstUtils.assertTableEquals;

public class TestIncrementalReleaseFilter extends LiveTableTestCase {
    public void testSimple() {
        final Table source =
            TableTools.newTable(TableTools.intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        TableTools.show(source);

        final IncrementalReleaseFilter incrementalReleaseFilter =
            new IncrementalReleaseFilter(2, 1);
        final Table filtered = source.where(incrementalReleaseFilter);

        TableTools.show(filtered);
        assertEquals(2, filtered.size());

        for (int ii = 0; ii <= 10; ++ii) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);

            TableTools.show(filtered);
            assertEquals(Math.min(3 + ii, 10), filtered.size());
        }
    }

    public void testBigTable() {
        final Table sourcePart = TableTools.emptyTable(1_000_000_000L);
        final List<Table> sourceParts =
            IntStream.range(0, 20).mapToObj(x -> sourcePart).collect(Collectors.toList());
        final Table source = TableTools.merge(sourceParts);
        TableTools.show(source);

        final IncrementalReleaseFilter incrementalReleaseFilter =
            new IncrementalReleaseFilter(2, 10_000_000);
        final Table filtered = source.where(incrementalReleaseFilter);
        final Table flattened = filtered.flatten();

        assertEquals(2, filtered.size());

        int cycles = 0;
        while (filtered.size() < source.size()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);
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
        LiveTableMonitor.DEFAULT.setTargetCycleTime(100);
        final Table source = TableTools.emptyTable(1_000_000);
        TableTools.show(source);

        final AutoTuningIncrementalReleaseFilter incrementalReleaseFilter =
            new AutoTuningIncrementalReleaseFilter(0, 100, 1.1, true,
                new ClockTimeProvider(new RealTimeClock()));
        final Table filtered = source.where(incrementalReleaseFilter);

        final Table updated =
            LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> filtered.update("I=ii"));

        while (filtered.size() < source.size()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);
        }

        TableTools.show(updated);
    }

    private int testAutoTuneCycle(int cycleTime) {
        LiveTableMonitor.DEFAULT.setTargetCycleTime(cycleTime);
        final Table source = TableTools.emptyTable(10_000);
        TableTools.show(source);

        final AutoTuningIncrementalReleaseFilter incrementalReleaseFilter =
            new AutoTuningIncrementalReleaseFilter(0, 100, 1.1, true,
                new ClockTimeProvider(new RealTimeClock()));
        final Table filtered = source.where(incrementalReleaseFilter);

        final Table updated =
            LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> filtered.update(
                "I=io.deephaven.db.v2.utils.TestIncrementalReleaseFilter.sleepValue(100000, ii)"));

        int cycles = 0;
        while (filtered.size() < source.size()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);
            System.out.println(filtered.size() + " / " + updated.size());
            cycles++;
        }
        return cycles;
    }
}
