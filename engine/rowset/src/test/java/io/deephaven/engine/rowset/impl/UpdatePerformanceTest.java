package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.rsp.RspRangeIterator;


import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;
import java.util.zip.CRC32;

public class UpdatePerformanceTest {
    static final NumberFormat nf = NumberFormat.getInstance();
    static final DecimalFormat mf = new DecimalFormat("0.000 Mb");
    static {
        mf.setMinimumFractionDigits(3);
        mf.setGroupingUsed(true);
        mf.setGroupingSize(3);
    }

    static String nf(final long v) {
        return nf.format(v);
    }

    static String mf(final double v) {
        return mf.format(v);
    }

    interface UpdateStrategy {
        interface Factory {
            UpdateStrategy make();
        }

        Factory getFactory();

        // Since tests will run multiple times, and creation time is high (higher than individual operations),
        // we create the base RowSet once and copy it before every run of update.
        void cloneBase();

        TstValues.Builder baseBuilder();

        TstValues.Builder addBuilder();

        TstValues.Builder removeBuilder();

        void normalizeAddRemove();

        long baseSize();

        long addSize();

        long removeSize();

        long optimizerBait(); // some value calculated from the result to prevent the optimizer from removing the code.

        Runnable getRunner(int i);

        long getBaseCrc32();
    }

    static void updateCrc32(final CRC32 crc32, final long v) {
        for (int bi = 0; bi < 8; ++bi) {
            final int b = (int) ((0x00000000000000FFL) & (v >> 8 * bi));
            crc32.update(b);
        }
    }

    static class IndexUpdateStrategy implements UpdateStrategy {
        private WritableRowSet ix[] = new WritableRowSet[4];
        private final Runnable[] runners = new Runnable[] {
                getParallelRunner(), getSequentialRunner()
        };

        private TstValues.Builder builder(final int i) {
            return new TstValues.Builder() {
                private final RowSetBuilderRandom b = RowSetFactory.builderRandom();

                @Override
                public void add(long v) {
                    b.addKey(v);
                }

                @Override
                public void done() {
                    ix[i] = b.build();
                }
            };
        }

        @Override
        public Factory getFactory() {
            return () -> new IndexUpdateStrategy();
        }

        @Override
        public void cloneBase() {
            ix[0] = ix[3].copy();
        }

        @Override
        public TstValues.Builder baseBuilder() {
            return builder(3);
        }

        @Override
        public TstValues.Builder addBuilder() {
            return builder(1);
        }

        @Override
        public TstValues.Builder removeBuilder() {
            return builder(2);
        }

        @Override
        public void normalizeAddRemove() {
            ix[1].remove(ix[2]);
        }

        @Override
        public long baseSize() {
            return ix[3].size();
        }

        @Override
        public long addSize() {
            return ix[1].size();
        }

        @Override
        public long removeSize() {
            return ix[2].size();
        }

        @Override
        public long optimizerBait() {
            return ix[0].lastRowKey();
        }

        private Runnable getParallelRunner() {
            return new Runnable() {
                @Override
                public void run() {
                    ix[0].update(ix[1], ix[2]);
                }

                @Override
                public String toString() {
                    return "TrackingWritableRowSet Parallel Update";
                }
            };
        }

        private Runnable getSequentialRunner() {
            return new Runnable() {
                @Override
                public void run() {
                    ix[0].insert(ix[1]);
                    ix[0].remove(ix[2]);
                }

                @Override
                public String toString() {
                    return "TrackingWritableRowSet Sequential Update";
                }
            };
        }

        @Override
        public Runnable getRunner(int i) {
            return runners[i];
        }

        @Override
        public long getBaseCrc32() {
            final CRC32 crc32 = new CRC32();
            RowSet.RangeIterator it = ix[0].rangeIterator();
            while (it.hasNext()) {
                it.next();
                for (long v = it.currentRangeStart(); v <= it.currentRangeEnd(); ++v) {
                    updateCrc32(crc32, v);
                }
            }
            return crc32.getValue();
        }
    }

    static class RspBitmapUpdateStrategy implements UpdateStrategy {
        private RspBitmap rbs[] = new RspBitmap[4];

        private TstValues.Builder builder(final int i) {
            return new TstValues.Builder() {
                private RspBitmap r = new RspBitmap();

                @Override
                public void add(final long v) {
                    r.addUnsafe(v);
                }

                @Override
                public void done() {
                    r.finishMutationsAndOptimize();
                    rbs[i] = r;
                }
            };
        }

        @Override
        public Factory getFactory() {
            return () -> new RspBitmapUpdateStrategy();
        }

        @Override
        public void cloneBase() {
            rbs[0] = rbs[3].deepCopy();
        }

        @Override
        public TstValues.Builder baseBuilder() {
            return builder(3);
        }

        @Override
        public TstValues.Builder addBuilder() {
            return builder(1);
        }

        @Override
        public TstValues.Builder removeBuilder() {
            return builder(2);
        }

        @Override
        public void normalizeAddRemove() {
            rbs[1].andNotEquals(rbs[2]);
        }

        @Override
        public long baseSize() {
            return rbs[3].getCardinality();
        }

        @Override
        public long addSize() {
            return rbs[1].getCardinality();
        }

        @Override
        public long removeSize() {
            return rbs[2].getCardinality();
        }

        @Override
        public long optimizerBait() {
            return rbs[0].last();
        }

        private final Runnable runner = new Runnable() {
            @Override
            public void run() {
                rbs[0].orEquals(rbs[1]);
                rbs[0].andNotEquals(rbs[2]);
            }

            @Override
            public String toString() {
                return "RspBitmap Update";
            }
        };

        @Override
        public Runnable getRunner(int unused) {
            return runner;
        }

        @Override
        public long getBaseCrc32() {
            final CRC32 crc32 = new CRC32();
            final RspRangeIterator it = rbs[0].getRangeIterator();
            while (it.hasNext()) {
                it.next();
                final long s = it.start();
                final long e = it.end();
                for (long v = s; v <= e; ++v) {
                    updateCrc32(crc32, v);
                }
            }
            it.close();
            return crc32.getValue();
        }
    }

    public static void setupStrategy(final UpdateStrategy s, final int sz,
            final TstValues.Config c, final String pref, final boolean print) {
        final TstValues.Builder baseBuilder = s.baseBuilder();
        final TstValues.Builder addBuilder = s.addBuilder();
        final TstValues.Builder removeBuilder = s.removeBuilder();
        TstValues.setup3(baseBuilder, addBuilder, removeBuilder, sz, c);
        s.normalizeAddRemove();
        if (!print) {
            return;
        }
        System.out.println(pref + "base size = " + nf(s.baseSize()));
        System.out.println(pref + "add size = " + nf(s.addSize()));
        System.out.println(pref + "remove size = " + nf(s.removeSize()));
    }

    static long runAndGetSamples(final UpdateStrategy s, final int ri, final int runs, final PerfStats stats) {
        long trick = 0; // to prevent the optimizer from eliminating unused steps.
        final PerfMeasure pm = new PerfMeasure(false);
        for (int i = 0; i < runs; ++i) {
            s.cloneBase();
            pm.start();
            s.getRunner(ri).run();
            pm.mark();
            stats.sample(pm.dt());
            trick += s.optimizerBait();
        }
        return trick;
    }

    static double codeWarmup(final UpdateStrategy.Factory f, int[] rs) {
        final int steps = 11000;
        final Random ra = new Random();
        final int max = 1000 * 1000 * 1000;
        double sum = 0;
        long trick = 0;
        for (int i = 0; i < steps; ++i) {
            final UpdateStrategy st = f.make();
            TstValues.Builder b = st.baseBuilder();
            b.add(ra.nextInt(max));
            b.done();
            b = st.addBuilder();
            b.add(ra.nextInt(max));
            b.done();
            b = st.removeBuilder();
            b.add(ra.nextInt(max));
            b.done();
            final PerfStats s = new PerfStats(2);
            for (int ri : rs) {
                trick += runAndGetSamples(st, ri, 1, s);
            }
            sum += s.avg();
        }
        return sum / steps / trick;
    }

    final static boolean runIndexParallel = true;
    final static boolean runIndexSequential = false;
    final static boolean runRspBitmap = true;
    static final TstValues.Config configs[] = {TstValues.dense}; // { TstValues.sparse, TstValues.dense,
                                                                 // TstValues.asymmetric };
    final static boolean doCrc32Check = true;

    static final String me = UpdatePerformanceTest.class.getSimpleName();
    static final double s2ns = 1e9;

    static void runStep(
            final TstValues.Config c, final int sn, final UpdateStrategy[] ss, final int[][] rs,
            final String stepName, final int sz, final int runs, final boolean check, final boolean print) {
        final Runtime rt = Runtime.getRuntime();
        System.out.println(me + ": Running " + c.name + " " + stepName + " sz=" + nf(sz));
        final String pfx = me + "    ";
        System.out.println(pfx + "Building values...");
        final PerfMeasure pm = new PerfMeasure(true);
        for (int si = 0; si < sn; ++si) {
            pm.start();
            setupStrategy(ss[si], sz, c, pfx, print);
            pm.mark();
            final double dMb = pm.dm() / (1024.0 * 1024.0);
            if (print) {
                System.out.println(pfx + String.format(
                        "Building values for " + ss[si].getClass().getSimpleName() +
                                " done in %.3f secs, delta memory used %s",
                        pm.dt() / s2ns, mf(dMb)));
            }
            pm.reset();
        }
        final double factor = 1 / s2ns;
        final String pos = " " + c.name + " " + stepName + " rowSet len=" + nf(sz) + ", seconds:";
        final long t0 = System.nanoTime();
        final PerfStats pStats = new PerfStats(runs);
        for (int si = 0; si < sn; ++si) {
            for (int ri = 0; ri < rs[si].length; ++ri) {
                final PerfStats sStats = (si == 0 && ri == 0) ? pStats : new PerfStats(runs);
                final long trick = runAndGetSamples(ss[si], rs[si][ri], runs, sStats);
                if (print) {
                    sStats.compute();
                    sStats.print(pfx + ss[si].getRunner(rs[si][ri]).toString() + pos, factor);
                }
                System.out.println(pfx + "trick optimizer value = " + nf(trick));
                if (!(si == 0 && ri == 0)) {
                    PerfStats.comparePrint(
                            pStats, ss[0].getRunner(0).toString(), sStats, ss[si].getRunner(rs[si][ri]).toString(),
                            pfx);
                }
            }
        }
        final long t1 = System.nanoTime();
        System.out.println(pfx + c.name + " " + stepName + " done in " + (t1 - t0) / s2ns + " s.");
        if (check) {
            for (int si = 0; si < sn; ++si) {
                for (int ri = 0; ri < rs[si].length; ++ri) {
                    ss[si].cloneBase();
                    ss[si].getRunner(rs[si][ri]).run();
                    final long ct0 = System.nanoTime();
                    final long crc32 = ss[si].getBaseCrc32();
                    final long ct1 = System.nanoTime();
                    System.out.println(
                            pfx + ss[si].getRunner(rs[si][ri]).toString() + " crc32=" + nf(crc32) +
                                    " done in " + (ct1 - ct0) / s2ns + " s.");
                }
            }
        }
        rt.gc();
    }

    // Having separate warmup and full methods helps separate them in JProfiler.
    static void runStepWarmup(final TstValues.Config c, final int sn, final UpdateStrategy ss[], final int[][] rs,
            final int sz, final int runs) {
        runStep(c, sn, ss, rs, "warmup", sz, runs, false, false);
    }

    static void runStepFull(final TstValues.Config c, final int sn, final UpdateStrategy ss[], final int[][] rs,
            final int sz, final int runs, final boolean check) {
        runStep(c, sn, ss, rs, "full test", sz, runs, check, true);
    }

    static void run(
            final TstValues.Config c, final int sn, final UpdateStrategy[] ss, final int[][] rs,
            final int warmupSz, final int warmupRuns, final int fullSz, final int fullRuns, final boolean check) {
        runStepWarmup(c, sn, ss, rs, warmupSz, warmupRuns);
        runStepFull(c, sn, ss, rs, fullSz, fullRuns, check);
    }

    public static void main(String[] args) {
        System.out.println(me + " PerfMeasure.conf() = " + PerfMeasure.conf());
        final int maxStrategies = 4;
        final UpdateStrategy[] ss = new UpdateStrategy[maxStrategies];
        final int[][] rs = new int[maxStrategies][];
        int sn = 0;
        if (runIndexParallel || runIndexSequential) {
            ss[sn] = new IndexUpdateStrategy();
            rs[sn] = new int[runIndexParallel && runIndexSequential ? 2 : 1];
            int rsj = 0;
            if (runIndexParallel) {
                rs[sn][rsj++] = 0;
            }
            if (runIndexSequential) {
                rs[sn][rsj++] = 1;
            }
            ++sn;
        }
        if (runRspBitmap) {
            ss[sn] = new RspBitmapUpdateStrategy();
            rs[sn] = new int[] {0};
            ++sn;
        }
        System.out.println(me + ": Running code warmup...");
        for (int si = 0; si < sn; ++si) {
            final long t0 = System.nanoTime();
            final double wo = codeWarmup(ss[si].getFactory(), rs[si]);
            final long t1 = System.nanoTime();
            final long dt = t1 - t0;
            System.out.println(me + ": " + ss[si].getClass().getSimpleName() + " Code warmup ran in " +
                    dt / s2ns + " seconds, output=" + wo);
        }
        final int warmupSz = 1 * 1000 * 1000;
        final int warmupRuns = 200;
        final int fullSz = 50 * 1000 * 1000;
        final int fullRuns = 40;
        for (TstValues.Config c : configs) {
            run(c, sn, ss, rs, warmupSz, warmupRuns, fullSz, fullRuns, doCrc32Check);
        }
    }
}
