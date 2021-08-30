package io.deephaven.benchmarking.generator.random;

import java.util.Random;

/**
 * ExtendedRandom is a helper to facilitate the use of various concrete {@link Random} implementations, while providing
 * a consistent interface.
 */
public abstract class ExtendedRandom {
    public final double nextDouble(double origin, double bound) {
        double r = nextDouble();
        if (origin < bound) {
            r = r * (bound - origin) + origin;
            if (r >= bound) // correct for rounding
                r = Double.longBitsToDouble(Double.doubleToLongBits(bound) - 1);
        }
        return r;
    }

    public final long nextLong(long origin, long bound) {
        long r = nextLong();
        if (origin < bound) {
            long n = bound - origin, m = n - 1;
            if ((n & m) == 0L) // power of two
                r = (r & m) + origin;
            else if (n > 0L) { // reject over-represented candidates
                for (long u = r >>> 1; // ensure nonnegative
                        u + m - (r = u % n) < 0L; // rejection check
                        u = nextLong() >>> 1) // retry
                ;
                r += origin;
            } else { // range not representable as long
                while (r < origin || r >= bound)
                    r = nextLong();
            }
        }
        return r;
    }

    public final int nextInt(int origin, int bound) {
        if (origin < bound) {
            int n = bound - origin;
            if (n > 0) {
                return nextInt(n) + origin;
            } else { // range not representable as int
                int r;
                do {
                    r = nextInt();
                } while (r < origin || r >= bound);
                return r;
            }
        } else {
            return nextInt();
        }
    }

    public abstract double nextDouble();

    public abstract long nextLong();

    public abstract int nextInt();

    public abstract int nextInt(int n);
}
