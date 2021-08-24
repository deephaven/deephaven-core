package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.db.v2.utils.rsp.RspRangeIterator;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;
import java.util.zip.CRC32;

public final class IterPerformanceTest {
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

    interface IterStrategy {
        interface Factory {
            IterStrategy make();
        }
        interface ValuesBuilder {
            void add(long v);

            void done();
        }

        IterStrategy.Factory getFactory();

        ValuesBuilder builder();

        // Since tests will run multiple times, and creation time is high (higher than individual
        // operations),
        // we create the base index one and clone it before every run of update.
        long getBaseCrc32(long toBeOred);

        long size();
    }

    static void updateCrc32(final CRC32 crc32, final long v) {
        for (int bi = 0; bi < 8; ++bi) {
            final int b = (int) ((0x00000000000000FFL) & (v >> 8 * bi));
            crc32.update(b);
        }
    }

    static abstract class IndexIterStrategy implements IterStrategy {
        protected Index ix;

        @Override
        public ValuesBuilder builder() {
            return new ValuesBuilder() {
                private final Index.RandomBuilder b = Index.FACTORY.getRandomBuilder();

                @Override
                public void add(long v) {
                    b.addKey(v);
                }

                @Override
                public void done() {
                    ix = b.getIndex();
                }
            };
        }

        @Override
        public long size() {
            return ix.size();
        }
    }

    static class IndexRangeIterStrategy extends IndexIterStrategy {
        @Override
        public long getBaseCrc32(final long x) {
            final CRC32 crc32 = new CRC32();
            final Index.RangeIterator it = ix.rangeIterator();
            while (it.hasNext()) {
                it.next();
                for (long v = it.currentRangeStart(); v <= it.currentRangeEnd(); ++v) {
                    updateCrc32(crc32, v | x);
                }
            }
            return crc32.getValue();
        }

        @Override
        public Factory getFactory() {
            return () -> new IndexRangeIterStrategy();
        }

        @Override
        public String toString() {
            return "IndexRange";
        }
    }

    static class IndexBasicIterStrategy extends IndexIterStrategy {
        @Override
        public long getBaseCrc32(final long x) {
            final CRC32 crc32 = new CRC32();
            final Index.Iterator it = ix.iterator();
            while (it.hasNext()) {
                final long v = it.nextLong();
                updateCrc32(crc32, v | x);
            }
            return crc32.getValue();
        }

        @Override
        public Factory getFactory() {
            return () -> new IndexBasicIterStrategy();
        }

        @Override
        public String toString() {
            return "IndexBasic";
        }
    }

    static class IndexForEachStrategy extends IndexIterStrategy {
        @Override
        public long getBaseCrc32(final long x) {
            final CRC32 crc32 = new CRC32();
            ix.forEachLong((long v) -> {
                updateCrc32(crc32, v | x);
                return true;
            });
            return crc32.getValue();
        }

        @Override
        public Factory getFactory() {
            return () -> new IndexForEachStrategy();
        }

        @Override
        public String toString() {
            return "IndexForEach";
        }
    }

    static class RspBitmapIterStrategy implements IterStrategy {
        private RspBitmap rb;

        @Override
        public IterStrategy.ValuesBuilder builder() {
            return new ValuesBuilder() {
                private RspBitmap r = new RspBitmap();

                @Override
                public void add(final long v) {
                    r.addUnsafeNoWriteCheck(v);
                }

                @Override
                public void done() {
                    r.finishMutationsAndOptimize();
                    rb = r;
                }
            };
        }

        @Override
        public Factory getFactory() {
            return () -> new RspBitmapIterStrategy();
        }

        @Override
        public long getBaseCrc32(final long x) {
            final CRC32 crc32 = new CRC32();
            final RspRangeIterator it = rb.getRangeIterator();
            while (it.hasNext()) {
                it.next();
                final long s = it.start();
                final long e = it.end();
                for (long v = s; v <= e; ++v) {
                    updateCrc32(crc32, v | x);
                }
            }
            it.close();
            return crc32.getValue();
        }

        @Override
        public long size() {
            return rb.getCardinality();
        }

        @Override
        public String toString() {
            return "RspBitmap";
        }
    }

    private static class ValuesBuilder {
        final IterStrategy.ValuesBuilder b;
        int clusterMid;

        ValuesBuilder(final int clusterMid, final IterStrategy.ValuesBuilder b) {
            this.b = b;
            this.clusterMid = clusterMid;
        }

        long populateFirstArgStep(final int jumpPropOneIn, final int d, final int halfClusterWidth,
            final Random r) {
            final long k;
            if (r.nextInt(jumpPropOneIn) == 0) {
                k = clusterMid = halfClusterWidth + r.nextInt(d);
            } else {
                k = clusterMid + r.nextInt(halfClusterWidth);
            }
            b.add(k);
            return k;
        }

        void populateSecondArgStep(final int sizePropOneIn, final int sharePropOneIn, final long k,
            int cluster1Mid, final int halfClusterWidth, final Random r) {
            if (sizePropOneIn != 1 && r.nextInt(sizePropOneIn) != 0) {
                return;
            }
            final long k2;
            if (r.nextInt(sharePropOneIn) == 0) {
                k2 = k;
                clusterMid = cluster1Mid;
            } else {
                k2 = clusterMid + r.nextInt(halfClusterWidth);
            }
            b.add(k2);
        }
    }

    private static class Config {
        Config(final String name, final int min, final int max, final int clusterWidth,
            final int sizePropOneIn,
            final int sharePropOneIn, final int jumpPropOneIn) {
            this.name = name;
            this.clusterWidth = clusterWidth;
            this.sizePropOneIn = sizePropOneIn;
            this.sharePropOneIn = sharePropOneIn;
            this.jumpPropOneIn = jumpPropOneIn;
            this.min = min;
            this.max = max;
        }

        private final String name;
        private final int clusterWidth;
        private final int sizePropOneIn;
        private final int sharePropOneIn;
        private final int jumpPropOneIn;
        private final int min;
        private final int max;
    };

    private static final Config sparse =
        new Config("sparse", 10, 300000000, 50, 1, 1000, 25);
    private static final Config dense =
        new Config("dense", 20, 30000000, 20, 1, 3, 20);
    private static final Config asymmetric =
        new Config("asymmetric", 10, 300000000, 30000000, 160000, 1000, 25);

    public static void setupStrategy(final IterStrategy s, final int sz, final Config c,
        final String pref, final boolean print) {
        final int halfClusterWidth = c.clusterWidth / 2;
        final IterStrategy.ValuesBuilder b = s.builder();
        final ValuesBuilder vb = new ValuesBuilder(c.min + halfClusterWidth, b);
        final Random r = new Random(0);
        final int d = c.max - c.min + 1 - c.clusterWidth;
        for (int i = 0; i < sz; ++i) {
            final long k = vb.populateFirstArgStep(c.jumpPropOneIn, d, halfClusterWidth, r);
            vb.populateSecondArgStep(c.sizePropOneIn, c.sharePropOneIn, k, vb.clusterMid,
                halfClusterWidth, r);
        }
        b.done();
        if (!print) {
            return;
        }
        System.out.println(pref + "b size = " + nf(s.size()));
    }

    static long runAndGetSamples(final IterStrategy s, final int runs, final PerfStats stats) {
        long trick = 0; // to prevent the optimizer from eliminating unused steps.
        final PerfMeasure pm = new PerfMeasure(false);
        for (int i = 0; i < runs; ++i) {
            pm.start();
            final long r = s.getBaseCrc32(trick);
            pm.mark();
            stats.sample(pm.dt());
            trick ^= r;
        }
        return trick;
    }

    static double codeWarmup(final IterStrategy.Factory f) {
        final int steps = 11000;
        final Random ra = new Random();
        final int max = 1000 * 1000 * 1000;
        double sum = 0;
        long trick = 0;
        for (int i = 0; i < steps; ++i) {
            final IterStrategy st = f.make();
            IterStrategy.ValuesBuilder b = st.builder();
            b.add(ra.nextInt(max));
            b.done();
            final PerfStats s = new PerfStats(2);
            trick += runAndGetSamples(st, 1, s);
            sum += s.avg();
        }
        return sum / steps / trick;
    }

    final static boolean runIndexBasic = true;
    final static boolean runIndexRange = true;
    final static boolean runIndexForEach = true;
    final static boolean runRspBitmap = true;
    static final Config configs[] = {sparse, dense, asymmetric};
    static final String me = IterPerformanceTest.class.getSimpleName();
    static final double s2ns = 1e9;

    static void runStep(
        final Config c, final int sn, final IterStrategy[] ss,
        final String stepName, final int sz, final int runs, final boolean print) {
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
                    "Building values for " + ss[si].toString() +
                        " done in %.3f secs, delta memory used %s",
                    pm.dt() / s2ns, mf(dMb)));
            }
            pm.reset();
        }
        final double factor = 1 / s2ns;
        final String pos = " " + c.name + " " + stepName + " index len=" + nf(sz) + ", seconds:";
        final long t0 = System.nanoTime();
        final PerfStats pStats = new PerfStats(runs);
        for (int si = 0; si < sn; ++si) {
            final PerfStats sStats = (si == 0) ? pStats : new PerfStats(runs);
            final long trick = runAndGetSamples(ss[si], runs, sStats);
            if (print) {
                sStats.compute();
                sStats.print(pfx + ss[si].toString() + pos, factor);
            }
            System.out.println(pfx + "trick optimizer value = " + nf(trick));
            if (si != 0) {
                PerfStats.comparePrint(
                    pStats, ss[0].toString(), sStats, ss[si].toString(), pfx);
            }
        }
        final long t1 = System.nanoTime();
        System.out.println(pfx + c.name + " " + stepName + " done in " + (t1 - t0) / s2ns + " s.");
        rt.gc();
    }

    // Having separate warmup and full methods helps separate them in JProfiler.
    static void runStepWarmup(final Config c, final int sn, final IterStrategy ss[],
        final int sz, final int runs) {
        runStep(c, sn, ss, "warmup", sz, runs, false);
    }

    static void runStepFull(final Config c, final int sn, final IterStrategy ss[],
        final int sz, final int runs) {
        runStep(c, sn, ss, "full test", sz, runs, true);
    }

    static void run(
        final Config c, final int sn, final IterStrategy[] ss,
        final int warmupSz, final int warmupRuns, final int fullSz, final int fullRuns) {
        runStepWarmup(c, sn, ss, warmupSz, warmupRuns);
        runStepFull(c, sn, ss, fullSz, fullRuns);
    }

    public static void main(String[] args) {
        System.out.println(me + " PerfMeasure.conf() = " + PerfMeasure.conf());
        final int maxStrategies = 6;
        final IterStrategy[] ss = new IterStrategy[maxStrategies];
        int sn = 0;
        if (runIndexBasic) {
            ss[sn] = new IndexBasicIterStrategy();
            ++sn;
        }
        if (runIndexRange) {
            ss[sn] = new IndexRangeIterStrategy();
            ++sn;
        }
        if (runIndexForEach) {
            ss[sn] = new IndexForEachStrategy();
            ++sn;
        }
        if (runRspBitmap) {
            ss[sn] = new RspBitmapIterStrategy();
            ++sn;
        }
        System.out.println(me + ": Running code warmup...");
        for (int si = 0; si < sn; ++si) {
            final long t0 = System.nanoTime();
            final double wo = codeWarmup(ss[si].getFactory());
            final long t1 = System.nanoTime();
            final long dt = t1 - t0;
            System.out.println(me + ": " + ss[si].toString() + " Code warmup ran in " +
                dt / s2ns + " seconds, output=" + wo);
        }
        final int warmupSz = 1 * 1000 * 1000;
        final int warmupRuns = 20;
        final int fullSz = 50 * 1000 * 1000;
        final int fullRuns = 20;
        for (Config c : configs) {
            run(c, sn, ss, warmupSz, warmupRuns, fullSz, fullRuns);
        }
    }
}
