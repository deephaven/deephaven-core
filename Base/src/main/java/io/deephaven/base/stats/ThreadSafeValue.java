//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.stats;

import io.deephaven.base.AtomicUtil;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class ThreadSafeValue extends Value {
    private static final AtomicLongFieldUpdater<Value> N_UPDATER = AtomicLongFieldUpdater.newUpdater(Value.class, "n");
    private static final AtomicLongFieldUpdater<Value> SUM_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Value.class, "sum");
    private static final AtomicLongFieldUpdater<Value> SUM2_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Value.class, "sum2");
    private static final AtomicLongFieldUpdater<Value> MAX_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Value.class, "max");
    private static final AtomicLongFieldUpdater<Value> MIN_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Value.class, "min");

    public ThreadSafeValue(long now) {
        super(now);
    }

    protected ThreadSafeValue(History history) {
        super(history);
    }

    @Override
    public void sample(final long x) {
        N_UPDATER.incrementAndGet(this);
        SUM_UPDATER.addAndGet(this, x);
        SUM2_UPDATER.addAndGet(this, x * x);
        last = x;
        if (x > max) {
            AtomicUtil.setMax(this, MAX_UPDATER, x);
        }
        if (x < min) {
            AtomicUtil.setMin(this, MIN_UPDATER, x);
        }
    }
}
