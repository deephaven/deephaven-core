package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.function.LongSupplier;

public class RowSetBuilderSequentialPerfTest {
    private static final NumberFormat sizeFormat = NumberFormat.getInstance();
    private static final DecimalFormat secsFormat = new DecimalFormat("0.000 s");
    static {
        secsFormat.setMinimumFractionDigits(3);
        secsFormat.setGroupingUsed(true);
        secsFormat.setGroupingSize(3);
    }

    private static String sizeFormat(final long v) {
        return sizeFormat.format(v);
    }

    private static String secondsFormat(final double v) {
        return secsFormat.format(v);
    }

    private static final int sz = 10 * 1000 * 1000;
    private static final RowSetBuilderSequential[] sbs = new RowSetBuilderSequential[sz];

    private static void clear() {
        for (int i = 0; i < sz; ++i) {
            sbs[i] = null;
        }
    }

    private static long doRspRun() {
        long bh = 0;
        for (int j = 0; j < sz; ++j) {
            sbs[j] = RowSetFactory.builderSequential();
        }
        for (int j = 0; j < sz; ++j) {
            sbs[j].appendKey(j);
            bh ^= sbs[j].build().firstRowKey();
        }
        return bh;
    }

    private static long runAndGetSamples(final int runs, final PerfStats stats, final LongSupplier runner) {
        long trick = 0; // to prevent the optimizer from eliminating unused steps.
        final PerfMeasure pm = new PerfMeasure(false);
        for (int i = 0; i < runs; ++i) {
            pm.start();
            final long res = runner.getAsLong();
            pm.mark();
            trick ^= res;
            if (stats != null) {
                stats.sample(pm.dt());
            }
            pm.reset();
        }
        return trick;
    }

    private static class PerfTest {
        public final LongSupplier runner;
        public final String name;

        public PerfTest(final LongSupplier runner, final String name) {
            this.runner = runner;
            this.name = name;
        }
    }

    private static final double s2ns = 1e9;

    private static void run(final int warmupRuns, final int fullRuns) {
        final PerfTest[] ts = {
                new PerfTest(RowSetBuilderSequentialPerfTest::doRspRun, "Rsp"),
        };
        final int runs[] = {warmupRuns, fullRuns};
        for (PerfTest t : ts) {
            clear();
            for (int r = 0; r < runs.length; ++r) {
                final String tname = t.name + " " + ((r == 0) ? "warmup" : "full") + " size=" + sizeFormat(sz);
                final long ts0 = System.nanoTime();
                System.out.println("Running " + tname + " ...");
                final int count = runs[r];
                PerfStats stats = null;
                if (r != 0) {
                    stats = new PerfStats(count);
                }
                final long res = runAndGetSamples(count, stats, t.runner);
                final long ts1 = System.nanoTime();
                System.out.println(
                        tname + " ran in " + secondsFormat((ts1 - ts0) / s2ns) + ", optimizer trick result = " + res);
                if (stats != null) {
                    stats.compute();
                    final double factor = 1 / s2ns;
                    stats.print(tname + " stats in seconds: ", factor);
                }
            }
        }
    }

    private static final String me = RowSetBuilderSequentialPerfTest.class.getSimpleName();

    public static void main(String[] args) {
        System.out.println(me + " PerfMeasure.conf() = " + PerfMeasure.conf());
        final int warmupRuns = 1;
        final int fullRuns = 5;
        run(warmupRuns, fullRuns);
    }
}
