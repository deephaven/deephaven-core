/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

public class History {

    public static final long[] INTERVALS = {1000, 10000, 60000, 300000, 900000, Long.MAX_VALUE};
    public static final String[] INTERVAL_NAMES = {"1s", "10s", "1m", "5m", "15m", "total"};

    public static final int INTERVAL_1S_INDEX = 0;
    public static final int INTERVAL_10S_INDEX = 1;
    public static final int INTERVAL_1M_INDEX = 2;
    public static final int INTERVAL_5M_INDEX = 3;
    public static final int INTERVAL_15M_INDEX = 4;
    public static final int INTERVAL_TOTAL_INDEX = 5;

    public static final int DEPTH = 2; // min is 2 (current partial interval + last full interval), can be larger to
                                       // retain more history

    public static int intervalIndex(String s) {
        for (int i = 0; i < INTERVAL_NAMES.length; ++i) {
            if (s.equals(INTERVAL_NAMES[i])) {
                return i;
            }
        }
        return -1;
    }

    private long[][] n = new long[INTERVALS.length][DEPTH];
    private long[][] last = new long[INTERVALS.length][DEPTH];
    private long[][] sum = new long[INTERVALS.length][DEPTH];
    private long[][] sum2 = new long[INTERVALS.length][DEPTH];
    private long[][] max = new long[INTERVALS.length][DEPTH];
    private long[][] min = new long[INTERVALS.length][DEPTH];

    // points to the "zero" depth for each interval
    private int[] currentDepth = new int[INTERVALS.length];

    // next update time for each interval
    private long[] nextUpdate = new long[INTERVALS.length];

    /**
     * Construct a new history, to start recording from now
     */
    public History(long now) {
        for (int i = 0; i < INTERVALS.length; ++i) {
            for (int d = 0; d < DEPTH; ++d) {
                max[i][d] = Long.MIN_VALUE;
                min[i][d] = Long.MAX_VALUE;
            }
        }
        for (int i = 0; i < INTERVALS.length; ++i) {
            nextUpdate[i] = (now + INTERVALS[i]) - (now % INTERVALS[i]);
        }
    }

    /**
     * Update the depth-zero history for all intervals, and begin new intervals for those whose depth-zero histories are
     * now in the past.
     * 
     * @return the highest-numbered interval which has been newly started.
     */
    public int update(Value v, long now) {
        int topInterval = 0;
        long vn = v.n;
        long vlast = v.last;
        long vsum = v.sum;
        long vsum2 = v.sum2;
        long vmin = v.min;
        long vmax = v.max;
        for (int i = 0; i < INTERVALS.length; ++i) {
            // update the values
            int d = currentDepth[i];
            n[i][d] += vn;
            last[i][d] = vlast;
            sum[i][d] += vsum;
            sum2[i][d] += vsum2;
            if (vmax > max[i][d]) {
                max[i][d] = vmax;
            }
            if (vmin < min[i][d]) {
                min[i][d] = vmin;
            }

            // if this interval has passed, bump the depth and zero the new one out
            if (now >= nextUpdate[i]) {
                topInterval = i;
                if (--d < 0) {
                    d = DEPTH - 1;
                }
                currentDepth[i] = d;
                n[i][d] = 0;
                last[i][d] = 0;
                sum[i][d] = 0;
                sum2[i][d] = 0;
                max[i][d] = Long.MIN_VALUE;
                min[i][d] = Long.MAX_VALUE;
                nextUpdate[i] += INTERVALS[i];
            }
        }
        return topInterval;
    }

    /**
     * Return the last value for the given interval and depth.
     */
    public long getLast(int i, int d) {
        return last[i][(currentDepth[i] + d) % DEPTH];
    }

    /**
     * Return the sum value for the given interval and depth.
     */
    public long getSum(int i, int d) {
        return sum[i][(currentDepth[i] + d) % DEPTH];
    }

    /**
     * Return the sum2 value for the given interval and depth.
     */
    public long getSum2(int i, int d) {
        return sum2[i][(currentDepth[i] + d) % DEPTH];
    }

    /**
     * Return the N value for the given interval and depth.
     */
    public long getN(int i, int d) {
        return n[i][(currentDepth[i] + d) % DEPTH];
    }

    /**
     * Return the max value for the given interval and depth.
     */
    public long getMax(int i, int d) {
        return max[i][(currentDepth[i] + d) % DEPTH];
    }

    /**
     * Return the min value for the given interval and depth.
     */
    public long getMin(int i, int d) {
        return min[i][(currentDepth[i] + d) % DEPTH];
    }

    /**
     * Return the average value for the given interval and depth.
     */
    public long getAvg(int i, int d) {
        long n = getN(i, d);
        return n == 0 ? 0 : getSum(i, d) / n;
    }

    /**
     * Return the stdev value for the given interval and depth.
     */
    public long getStdev(int i, int d) {
        long nSamples = getN(i, d);
        double sum = getSum(i, d);
        double sum2 = getSum2(i, d);

        double ave = sum / nSamples;
        if (nSamples <= 1) {
            return 0;
        }
        double var = sum2 / (nSamples - 1) - ave * ave * ((double) nSamples / (double) (nSamples - 1));
        if (var < 0.0) {
            return -1; // if sum2 goes overflow on us variance could go nuts ... be safe and log a junk variance value
                       // instead of exploding
        } else {
            return (long) Math.ceil(Math.sqrt(var));
        }
    }
}
