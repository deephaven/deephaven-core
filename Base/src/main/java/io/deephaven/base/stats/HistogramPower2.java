/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

import io.deephaven.base.Function;

/**
 * This class accumulates samples in a 64 bin histogram with the property that for a sample value of
 * n, the bin index will be log2(n)+1 (offset by 1) since there are no unsinged numbers, and placing
 * negative and 0 values in bin 0 preserves order, with maximum appearing in bin index 63 (max pos
 * numbers)
 */
public class HistogramPower2 extends Value {

    private static final int LONG_WIDTH = 64;
    public static char TYPE_TAG = 'N';

    private long[] histo = new long[LONG_WIDTH];
    private long m_samples = 0;

    public HistogramPower2(long now) {
        super(now);
    }

    public void sample(long n) {
        super.sample(n);
        int v = (LONG_WIDTH - Long.numberOfLeadingZeros(n));
        if (v >= LONG_WIDTH) {
            v = 0;
        } // negative and 0 go into bin 0
        ++histo[v]; // all others go into bin number log2(n) + 1
        ++m_samples;
    }

    private final ThreadLocal<StringBuilder> threadLocalStringBuilder =
        new ThreadLocal<StringBuilder>() {
            @Override
            protected StringBuilder initialValue() {
                return new StringBuilder(100);
            }
        };

    public String getHistogramString() {
        if (m_samples == 0) {
            return "<empty profile>";
        }
        StringBuilder hs = threadLocalStringBuilder.get();
        hs.setLength(0);
        hs.append("Samples: ").append(m_samples);
        int topIdx = LONG_WIDTH - 1;
        int idx = 0;
        while ((topIdx > 0) && (histo[topIdx] == 0)) {
            topIdx--;
        }
        while (idx < topIdx && histo[idx] == 0) {
            idx++;
        }
        for (; idx <= topIdx; idx++) {
            hs.append(",[").append(idx).append("]=").append(histo[idx]);
        }
        return hs.toString();
    }

    public long[] getHistogram() {
        return histo;
    }

    public void clear() {
        super.reset();
        m_samples = 0;
        for (int i = 0; i < LONG_WIDTH; ++i) {
            histo[i] = 0;
        }
    }

    public void reset() {
        super.reset();
        // do nothing here!
        // behaviour is cumulative over all intervals. if undesired, call clear() here
    }

    public char getTypeTag() {
        return TYPE_TAG;
    }

    public static final Function.Unary<HistogramPower2, Long> FACTORY =
        new Function.Unary<HistogramPower2, Long>() {
            public HistogramPower2 call(Long now) {
                return new HistogramPower2(now);
            }
        };
}
