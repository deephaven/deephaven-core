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
