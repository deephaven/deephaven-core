package io.deephaven.db.v2.utils;

import java.util.Random;

public class TestValues {
    public interface Builder {
        void add(long v);

        void done();
    }

    public static class Context {
        private final Builder b;
        private int clusterMid;

        public Context(final int clusterMid, final Builder b) {
            this.b = b;
            this.clusterMid = clusterMid;
        }

        public long populateFirstArgStep(final int jumpPropOneIn, final int d, int halfClusterWidth,
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

        public void populateSecondArgStep(final int sizePropOneIn, final int sharePropOneIn,
            final long k,
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

        public int getClusterMid() {
            return clusterMid;
        }

    }

    public static class Config {
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

        public final String name;
        public final int clusterWidth;
        public final int sizePropOneIn;
        public final int sharePropOneIn;
        public final int jumpPropOneIn;
        public final int min;
        public final int max;
    };

    public static final Config sparse =
        new Config("sparse", 10, 300000000, 50, 1, 1000, 25);
    public static final Config dense =
        new Config("dense", 20, 30000000, 20, 1, 3, 20);
    public static final Config asymmetric =
        new Config("asymmetric", 10, 300000000, 30000000, 160000, 1000, 25);

    public static void setup(final Builder b, final int sz, final TestValues.Config c) {
        final int halfClusterWidth = c.clusterWidth / 2;
        final TestValues.Context ctx = new TestValues.Context(c.min + halfClusterWidth, b);
        final Random r = new Random(0);
        final int d = c.max - c.min + 1 - c.clusterWidth;
        for (int i = 0; i < sz; ++i) {
            ctx.populateFirstArgStep(c.jumpPropOneIn, d, halfClusterWidth, r);
        }
        b.done();
    }

    public static void setup3(final Builder b1, final Builder b2, final Builder b3, final int sz,
        final TestValues.Config cf) {
        final int halfClusterWidth = cf.clusterWidth / 2;
        final TestValues.Context cx1 = new TestValues.Context(cf.min + halfClusterWidth, b1);
        final TestValues.Context cx2 = new TestValues.Context(cf.max + halfClusterWidth, b2);
        final TestValues.Context cx3 = new TestValues.Context(cf.max + halfClusterWidth, b3);
        final Random r = new Random(0);
        final int d = cf.max - cf.min + 1 - cf.clusterWidth;
        for (int i = 0; i < sz; ++i) {
            final long k = cx1.populateFirstArgStep(cf.jumpPropOneIn, d, halfClusterWidth, r);
            final TestValues.Context cx = (r.nextBoolean()) ? cx2 : cx3;
            cx.populateSecondArgStep(cf.sizePropOneIn, cf.sharePropOneIn, k, cx1.getClusterMid(),
                halfClusterWidth, r);
        }
        b1.done();
        b2.done();
        b3.done();
    }
}
