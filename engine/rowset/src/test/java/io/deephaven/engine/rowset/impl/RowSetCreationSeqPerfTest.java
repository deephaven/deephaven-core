package io.deephaven.engine.rowset.impl;

public class RowSetCreationSeqPerfTest {
    private final RowSetLike il;
    private final int sz;
    private final long seqMask;
    private final long singleMask;
    private final long startStep;
    private final long endStep;

    private static class Config {
        public Config(final String name, final int seqOneIn2Pow, final int singleOneInTwoPow) {
            this.name = name;
            this.seqOneIn2Pow = seqOneIn2Pow;
            this.singleOneInTwoPow = singleOneInTwoPow;
        }

        public final int seqOneIn2Pow;
        public final int singleOneInTwoPow;
        public final String name;
    }

    public RowSetCreationSeqPerfTest(final RowSetLike.Factory f, final Config c, final int sz) {
        il = f.make();
        this.sz = sz;
        if (c.seqOneIn2Pow < 0)
            throw new IllegalArgumentException("seqOneIn2Pow <= 0");
        if (c.singleOneInTwoPow < 0)
            throw new IllegalArgumentException("singleOneInTwoPow <= 0");
        if (c.seqOneIn2Pow == 0) {
            seqMask = 0;
            startStep = 2;
        } else {
            seqMask = (1 << (long) c.seqOneIn2Pow) - 1;
            startStep = 1;
        }
        if (c.singleOneInTwoPow == 0) {
            singleMask = 0;
            endStep = 1;
        } else {
            singleMask = (1 << (long) c.singleOneInTwoPow) - 1;
            endStep = 0;
        }
    }

    public void create() {
        long start = 1;
        long end = 2;
        for (int i = 0; i < sz; ++i) {
            il.addRange(start, end);
            start = end + startStep + (i & seqMask);
            end = start + endStep + (i & singleMask);
        }
        il.doneAdding();
    }

    public long lastKey() {
        return il.lastKey();
    }

    static long runAndGetSamples(
            final RowSetLike.Factory f, final Config c, final int sz, final int runs, final PerfStats stats,
            final String pfx, final boolean print) {
        final Runtime rt = Runtime.getRuntime();
        long lasts = 0; // to prevent the optimizer from eliminating unused steps.
        long tsum = 0;
        double minMb = Double.MAX_VALUE;
        for (int i = 0; i < runs; ++i) {
            final long t0 = System.currentTimeMillis();
            rt.gc();
            final long memPre = rt.totalMemory() - rt.freeMemory();
            final RowSetCreationSeqPerfTest t = new RowSetCreationSeqPerfTest(f, c, sz);
            t.create();
            final long lastKey = t.lastKey();
            final long t1 = System.currentTimeMillis();
            rt.gc();
            final long memPos = rt.totalMemory() - rt.freeMemory();
            lasts += lastKey;
            final double dMb = (memPos - memPre) / (1024.0 * 1024.0);
            if (minMb > dMb) {
                minMb = dMb;
            }
            final long dt = t1 - t0;
            stats.sample(dt);
            tsum += dt;
        }
        if (print) {
            System.out.println(String.format("%s done in %.3f seconds, min delta memory used %.3f Mb",
                    pfx, tsum / 1000.0, minMb));
        }
        return lasts;
    }

    static final String me = RowSetCreationSeqPerfTest.class.getSimpleName();

    static double codeWarmup() {
        final int steps = 500;
        final Config c = new Config("codeWarmup", 1, 1);
        long lasts = 0;
        double sum = 0;
        for (RowSetLike.Factory f : ilfs)
            for (int i = 0; i < steps; ++i) {
                final PerfStats s = new PerfStats(2);
                lasts += runAndGetSamples(f, c, 8 * 32, 1, s, "", false);
                sum += s.avg();
            }
        return sum / steps / lasts;
    }

    static void runStep(final Config c, final String stepName, final int sz, final int runs, final boolean print) {
        for (RowSetLike.Factory f : ilfs) {
            System.out.println(me + ": Running " + f.name() + " " + c.name + " " + stepName + " sz=" + sz);
            final PerfStats sStats = new PerfStats(runs);
            final String pfx = me + "    ";
            final String b = pfx + f.name() + " " + c.name + " " + stepName + " rowSet len=" + sz;
            final long lasts = runAndGetSamples(f, c, sz, runs, sStats, b, print);
            if (print) {
                sStats.compute();
                final String s = b + ", stats seconds:";
                final double factor = 1 / 1000.0;
                sStats.print(s, factor);
            }
            System.out.println(pfx + "trick optimizer value =" + lasts);
        }
    }

    static void run(final Config c, final int warmupSz, final int fullSz, final int runs) {
        runStep(c, "warmup", warmupSz, runs, false);
        runStep(c, "full test", fullSz, runs, true);
    }

    private static final Config c01 = new Config("c01", 0, 1);
    private static final Config c10 = new Config("c10", 1, 0);
    private static final Config c11 = new Config("c11", 1, 1);

    private static final Config[] configs = {c01, /* c10, c11 */ };

    private static final RowSetLike.Factory ilfs[] = {RowSetLike.mixedf, RowSetLike.pqf, RowSetLike.rspf};

    public static void main(String[] args) {
        System.out.println(me + ": Running code warmup...");
        final long t0 = System.currentTimeMillis();
        final double wo = codeWarmup();
        final long t1 = System.currentTimeMillis();
        final long dt = t1 - t0;
        System.out.println(me + ": Code warmup ran in " + dt / 1000.0 + " seconds, output = " + wo);
        final int warmupSz = 4 * 1000 * 1000;
        final int fullSz = 12 * 1000 * 1000;
        final int runs = 10;
        for (Config c : configs) {
            run(c, warmupSz, fullSz, runs);
        }
    }
}
