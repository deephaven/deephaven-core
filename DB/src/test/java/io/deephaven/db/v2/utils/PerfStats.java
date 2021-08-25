package io.deephaven.db.v2.utils;

import gnu.trove.list.array.TLongArrayList;

import java.text.NumberFormat;

public class PerfStats {
    private TLongArrayList samples;
    private double avg;
    private double stddev;

    public PerfStats(final int nsamples) {
        samples = new TLongArrayList(nsamples);
    }

    public void sample(final long v) {
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
    public long percentile(final double p) {
        if (p < 0.0 || p > 100.0)
            return 0;
        final int n = samples.size();
        final int i = (int) Math.ceil((n * p) / 100.0);
        return samples.get(i > 0 ? i - 1 : 0);
    }

    public static final int defaultPrintPs[] = {0, 5, 50, 90, 95, 99};

    public void print(final String pfx, final double factor) {
        print(pfx, factor, defaultPrintPs);
    }

    private String dFmt = "%.4f";

    public void setDecimalFormat(final String dFmt) {
        this.dFmt = dFmt;
    }

    public void print(final String pfx, final double factor, final int[] ps) {
        final double avg = avg() * factor;
        final double stddev = stddev() * factor;
        final StringBuffer sb = new StringBuffer(pfx);
        final NumberFormat nf = NumberFormat.getInstance();
        sb.append(String.format(" n=%s, avg=" + dFmt + " stddev=" + dFmt, nf.format(nsamples()),
            avg, stddev));
        for (int p : ps) {
            final double pct = percentile(p) * factor;
            sb.append(String.format(", p[%d]=" + dFmt, p, pct));
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
            sb.append(String.format("p[%d]=" + p1.dFmt, p, ratio));
            first = false;
        }
        sb.append(".");
        System.out.println(sb.toString());
    }
}
