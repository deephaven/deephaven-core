package io.deephaven.engine.rowset.impl;

import gnu.trove.list.array.TIntArrayList;
import io.deephaven.engine.rowset.RowSet;

import java.util.Map;
import java.util.TreeMap;

public class RowSetCreationRandomPerfTest {
    private final RowSetLike il;
    private final QuickDirtyRandom r;
    private final int sz;

    private static final int seed = 1;

    public RowSetCreationRandomPerfTest(final RowSetLike.Factory ilf, final int sz) {
        this.il = ilf.make();
        r = new QuickDirtyRandom(seed);
        this.sz = sz;
    }

    public void create() {
        for (int i = 0; i < sz; ++i) {
            il.addKey(r.curr());
            r.next();
        }
        il.doneAdding();
    }

    public RowSet getRowSet() {
        if (il instanceof RowSetLike.ActualRowSet) {
            return ((RowSetLike.ActualRowSet) il).getRowSet();
        }
        return null;
    }

    public long lastKey() {
        return il.lastKey();
    }

    private static final boolean doLeavesTypeStats = true;

    static long runAndGetSamples(
            final RowSetLike.Factory ilf,
            final int sz, final int runs, final PerfStats stats,
            final String pfx, final boolean print) {
        final Runtime rt = Runtime.getRuntime();
        long lasts = 0; // to prevent the optimizer from eliminating unused steps.
        long tsum = 0;
        double minMb = Double.MAX_VALUE;
        for (int i = 0; i < runs; ++i) {
            rt.gc();
            final long memPre = rt.totalMemory() - rt.freeMemory();
            RowSetCreationRandomPerfTest t = new RowSetCreationRandomPerfTest(ilf, sz);
            final long t0 = System.currentTimeMillis();
            t.create();
            final long t1 = System.currentTimeMillis();
            final long lk = t.lastKey();
            rt.gc();
            final long memPos = rt.totalMemory() - rt.freeMemory();
            lasts += lk;
            final double dMb = (memPos - memPre) / (1024.0 * 1024.0);
            if (minMb > dMb) {
                minMb = dMb;
            }
            final long dt = t1 - t0;
            stats.sample(dt);
            tsum += dt;
        }
        if (print) {
            System.out.println(String.format("%s done in %.3f seconds, min delta memory used %7.3f Mb",
                    pfx, tsum / 1000.0, minMb));
        }
        return lasts;
    }

    static final String me = RowSetCreationRandomPerfTest.class.getSimpleName();

    private static final RowSetLike.Factory ilfs[] = {RowSetLike.mixedf, RowSetLike.pqf, RowSetLike.rspf};

    static double codeWarmup() {
        final int steps = 500;
        long lasts = 0;
        double sum = 0;
        for (RowSetLike.Factory ilf : ilfs) {
            for (int i = 0; i < steps; ++i) {
                final PerfStats s = new PerfStats(2);
                lasts += runAndGetSamples(ilf, 8 * 64, 1, s, "", false);
                sum += s.avg();
            }
        }
        return sum / steps / lasts;
    }

    static void runStep(final String stepName, final int sz, final int runs, final boolean print) {
        final Map<String, PerfStats> ss = new TreeMap<>();
        final String pfx = me + "    ";
        int maxNameLen = 0;
        for (RowSetLike.Factory ilf : ilfs) {
            if (ilf.name().length() > maxNameLen) {
                maxNameLen = ilf.name().length();
            }
        }
        for (RowSetLike.Factory ilf : ilfs) {
            final String header = String.format("%-" + maxNameLen + "s %s",
                    ilf.name(), stepName + " sz=" + sz + " runs=" + runs);
            System.out.println(me + ": Running " + " " + header);
            final PerfStats stats = new PerfStats(runs);
            final String b = pfx + header;
            final long lasts = runAndGetSamples(ilf, sz, runs, stats, b, true);
            if (print) {
                stats.compute();
                final String s = b + ", stats seconds:";
                final double factor = 1 / 1000.0;
                stats.print(s, factor);
                ss.put(ilf.name(), stats);
            }
            System.out.println(pfx + "trick optimizer value =" + lasts);
        }
        if (print) {
            for (int i = 0; i < ilfs.length; ++i) {
                for (int j = i + 1; j < ilfs.length; ++j) {
                    final String ni = ilfs[i].name();
                    final String nj = ilfs[j].name();
                    PerfStats.comparePrint(ss.get(ni), ni, ss.get(nj), nj, pfx);
                }
            }
        }
    }

    static void run(final int warmupSz, final int warmupRuns, final int fullSz, final int fullRuns) {
        runStep("warmup", warmupSz, warmupRuns, false);
        runStep("full test", fullSz, fullRuns, true);
    }

    static class IntStats {
        private TIntArrayList samples;
        private double avg;
        private double stddev;

        public IntStats(final int nsamples) {
            samples = new TIntArrayList(nsamples);
        }

        public void sample(final int v) {
            samples.add(v);
        }

        public int nsamples() {
            return samples.size();
        }

        public double avg() {
            return avg;
        }

        public double stddev() {
            return stddev;
        }

        public void compute() {
            final int n = samples.size();
            long sumx = 0;
            long sumsqx = 0;
            for (int i = 0; i < n; ++i) {
                final long v = samples.get(i);
                sumx += v;
                sumsqx += v * v;
            }
            avg = sumx / (double) n;
            final double stddev2 = sumsqx / n + avg * avg;
            stddev = Math.sqrt(stddev2);
            samples.sort();
        }

        // assumption: before calling percentile compute was called (and thus the array is sorted).
        public int percentile(final double p) {
            if (p < 0.0 || p > 100.0)
                return 0;
            final int n = samples.size();
            final int i = (int) Math.ceil((n * p) / 100.0);
            return samples.get(i > 0 ? i - 1 : 0);
        }

        public static final int defaultPrintPs[] = {0, 5, 10, 25, 40, 45, 50, 55, 60, 75, 90, 95, 99};

        public void print(final String pfx) {
            print(pfx, defaultPrintPs);
        }

        public void print(final String pfx, final int[] ps) {
            final double avg = avg();
            final double stddev = stddev();
            StringBuffer sb = new StringBuffer(pfx);
            sb.append(String.format(" n=%d, avg=%.3f, stddev=%.3f", nsamples(), avg, stddev));
            for (int p : ps) {
                sb.append(String.format(", p[%d]=%d", p, percentile(p)));
            }
            System.out.println(sb.toString());
        }

        public static void comparePrint(
                final PerfStats p1, final String n1,
                final PerfStats p2, final String n2,
                final String pfx) {
            final StringBuilder sb = new StringBuilder(pfx);
            sb.append(n1);
            sb.append("/");
            sb.append(n2);
            sb.append(": ");
            boolean first = true;
            for (int p : PerfStats.defaultPrintPs) {
                final double pp1 = p1.percentile(p);
                final double pp2 = p2.percentile(p);
                final double ratio = (pp2 != 0.0) ? pp1 / pp2 : 0.0;
                if (!first) {
                    sb.append(", ");
                }
                sb.append(String.format("p[%d]=%.3f", p, ratio));
                first = false;
            }
            sb.append(".");
            System.out.println(sb.toString());
        }
    }

    public static void main(String[] args) {
        System.out.println(me + ": Running code warmup...");
        final long t0 = System.currentTimeMillis();
        final double wo = codeWarmup();
        final long t1 = System.currentTimeMillis();
        final long dt = t1 - t0;
        System.out.println(me + ": Code warmup ran in " + dt / 1000.0 + " seconds, output = " + wo);
        final int warmupSz = 12 * 1000 * 1000;
        final int warmupRuns = 1;
        final int fullSz = 12 * 1000 * 1000;
        final int fullRuns = 10;
        run(warmupSz, warmupRuns, fullSz, fullRuns);
    }
}
