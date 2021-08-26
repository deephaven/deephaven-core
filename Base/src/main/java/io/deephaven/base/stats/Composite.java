/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;

// --------------------------------------------------------------------
/**
 * A statistic that represents the composite or rolled-up value of a set of child statistics, as best as possible. This
 * statistic cannot be {@link #sample}d or {@link #increment}ed. Currently, calls to {@link #update} are silently
 * ignored, which also means that Composites are not logged. Thus a Composite is currently only useful for summaries in
 * web/JMX displays.
 */
public class Composite extends Value {

    private final Value[] m_values;

    // ----------------------------------------------------------------
    public Composite(long now, Value... values) {
        super(new CompositeHistory(now, checkValues(values)));
        m_values = values;
    }

    // ----------------------------------------------------------------
    private static Value[] checkValues(Value[] values) {
        Require.neqNull(values, "values", 1);
        Require.gtZero(values.length, "values.length", 1);
        Require.neqNull(values[0], "values[0]", 1);
        char typeTag = values[0].getTypeTag();
        for (int nIndex = 1; nIndex < values.length; nIndex++) {
            Require.neqNull(values[nIndex], "values[nIndex]", 1);
            Require.eq(values[nIndex].getTypeTag(), "values[nIndex].getTypeTag()", typeTag, "typeTag", 1);
        }
        return values;
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public void sample(long x) {
        throw Assert.statementNeverExecuted();
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public char getTypeTag() {
        return m_values[0].getTypeTag();
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public long getN() {
        long n = 0;
        for (Value value : m_values) {
            n += value.getN();
        }
        return n;
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public long getLast() {
        return m_values[0].getLast();
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public long getSum() {
        long sum = 0;
        for (Value value : m_values) {
            sum += value.getSum();
        }
        return sum;
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public long getSum2() {
        long sum2 = 0;
        for (Value value : m_values) {
            sum2 += value.getSum2();
        }
        return sum2;
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public long getMax() {
        long max = m_values[0].getMax();
        for (int nIndex = 1; nIndex < m_values.length; nIndex++) {
            max = Math.max(max, m_values[nIndex].getMax());
        }
        return max;
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public long getMin() {
        long min = m_values[0].getMin();
        for (int nIndex = 1; nIndex < m_values.length; nIndex++) {
            min = Math.min(min, m_values[nIndex].getMin());
        }
        return min;
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public History getHistory() {
        return super.getHistory();
    }

    // ----------------------------------------------------------------
    @Override // from Value
    public void update(Item item, ItemUpdateListener listener, long logInterval, long now, long appNow) {
        // composites are not updated (or logged)
    }

    // ################################################################

    private static class CompositeHistory extends History {

        private final History[] m_histories;

        // ------------------------------------------------------------
        public CompositeHistory(long now, Value[] values) {
            super(now);
            Require.neqNull(values, "values");
            Require.gtZero(values.length, "values.length");
            m_histories = new History[values.length];
            for (int nIndex = 0; nIndex < values.length; nIndex++) {
                Require.neqNull(values[nIndex], "values[nIndex]");
                m_histories[nIndex] = values[nIndex].getHistory();
            }
        }

        // ------------------------------------------------------------
        @Override // from History
        public int update(Value v, long now) {
            throw Assert.statementNeverExecuted();
        }

        // ------------------------------------------------------------
        @Override // from History
        public long getLast(int i, int d) {
            return m_histories[0].getLast(i, d);
        }

        // ------------------------------------------------------------
        @Override // from History
        public long getSum(int i, int d) {
            long sum = 0;
            for (History history : m_histories) {
                sum += history.getSum(i, d);
            }
            return sum;
        }

        // ------------------------------------------------------------
        @Override // from History
        public long getSum2(int i, int d) {
            long sum2 = 0;
            for (History history : m_histories) {
                sum2 += history.getSum2(i, d);
            }
            return sum2;
        }

        // ------------------------------------------------------------
        @Override // from History
        public long getN(int i, int d) {
            long n = 0;
            for (History history : m_histories) {
                n += history.getN(i, d);
            }
            return n;
        }

        // ------------------------------------------------------------
        @Override // from History
        public long getMax(int i, int d) {
            long max = m_histories[0].getMax(i, d);
            for (int nIndex = 1; nIndex < m_histories.length; nIndex++) {
                max = Math.max(max, m_histories[nIndex].getMax(i, d));
            }
            return max;
        }

        // ------------------------------------------------------------
        @Override // from History
        public long getMin(int i, int d) {
            long min = m_histories[0].getMin(i, d);
            for (int nIndex = 1; nIndex < m_histories.length; nIndex++) {
                min = Math.min(min, m_histories[nIndex].getMin(i, d));
            }
            return min;
        }
    }
}
