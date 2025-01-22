//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.stats;

import io.deephaven.base.AtomicUtil;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class Value {
    private static final AtomicLongFieldUpdater<Value> N_UPDATER = AtomicLongFieldUpdater.newUpdater(Value.class, "n");
    private static final AtomicLongFieldUpdater<Value> SUM_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Value.class, "sum");
    private static final AtomicLongFieldUpdater<Value> SUM2_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Value.class, "sum2");
    private static final AtomicLongFieldUpdater<Value> MAX_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Value.class, "max");
    private static final AtomicLongFieldUpdater<Value> MIN_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Value.class, "min");

    protected volatile long n = 0;
    protected volatile long last = 0;
    protected volatile long sum = 0;
    protected volatile long sum2 = 0;
    protected volatile long max = Long.MIN_VALUE;
    protected volatile long min = Long.MAX_VALUE;

    private boolean alwaysUpdated = false;

    // NOTE: If you are looking in here to determine what the output data columns mean:
    // n,sum,last,min,max,ave,sum2,stdev

    public long getN() {
        return n;
    }

    public long getLast() {
        return last;
    }

    public long getSum() {
        return sum;
    }

    public long getSum2() {
        return sum2;
    }

    public long getMax() {
        return max;
    }

    public long getMin() {
        return min;
    }

    protected final History history;

    public Value(long now) {
        history = new History(now);
    }

    protected Value(History history) {
        this.history = history;
    }

    public void sample(long x) {
        N_UPDATER.incrementAndGet(this);
        SUM_UPDATER.addAndGet(this, x);
        SUM2_UPDATER.addAndGet(this, x * x);
        last = x;
        if (x > max) {
            AtomicUtil.setMax(this, MAX_UPDATER, x);
        }
        if (x < min) {
            AtomicUtil.setMin(this, MAX_UPDATER, x);
        }
    }

    public void increment(long x) {
        sample(x);
    }

    public abstract char getTypeTag();

    public History getHistory() {
        return history;
    }

    public Value alwaysUpdated(boolean b) {
        alwaysUpdated = b;
        return this;
    }

    public void reset() {
        n = sum = sum2 = 0;
        max = Long.MIN_VALUE;
        min = Long.MAX_VALUE;
    }

    public void update(Item item, ItemUpdateListener listener, long logInterval, long now, long appNow) {
        int topInterval = history.update(this, now);
        reset();
        if (History.INTERVALS[topInterval] >= logInterval) {
            for (int i = 0; i <= topInterval; ++i) {
                if (History.INTERVALS[i] >= logInterval && history.getN(i, 1) > 0 || alwaysUpdated) {
                    if (listener != null) {
                        listener.handleItemUpdated(item, now, appNow, i, History.INTERVALS[i],
                                History.INTERVAL_NAMES[i]);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        final DecimalFormat format = new DecimalFormat("#,###");
        final DecimalFormat avgFormat = new DecimalFormat("#,###.###");


        if (n > 0) {
            final double std = Math.sqrt(n > 1 ? (sum2 - ((double) sum * sum / (double) n)) / (n - 1) : Double.NaN);
            final double avg = (double) sum / n;
            return String.format("Value{n=%,d, sum=%,d, max=%,d, min=%,d, avg=%,.3f, std=%,.3f}", n, sum, max, min, avg,
                    std);
        } else {
            return String.format("Value{n=%,d}", n);
        }
    }
}
