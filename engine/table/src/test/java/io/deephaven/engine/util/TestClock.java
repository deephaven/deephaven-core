//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.base.clock.ClockNanoBase;

public final class TestClock extends ClockNanoBase {
    public long now;

    public TestClock() {
        this(0L);
    }

    public TestClock(long nowNanos) {
        this.now = nowNanos;
    }

    @Override
    public long currentTimeNanos() {
        return now;
    }
}
