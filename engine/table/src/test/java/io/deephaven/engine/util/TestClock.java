//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.base.clock.ClockNanoBase;
import io.deephaven.time.DateTimeUtils;

import java.time.ZoneId;

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

    /**
     * Set the time.
     *
     * @param epochNanos the time to set in nanos since the epoch
     * @return this clock
     */
    public TestClock setNanos(long epochNanos) {
        this.now = epochNanos;
        return this;
    }

    /**
     * Set the time.
     *
     * @param epochMillis the time to set in millis since the epoch
     * @return this clock
     */
    public TestClock setMillis(long epochMillis) {
        this.now = DateTimeUtils.millisToNanos(epochMillis);
        return this;
    }

    /**
     * Add millis to the current time.
     *
     * @param millis the number of millis to add
     * @return this clock
     */
    public TestClock addMillis(long millis) {
        this.now += DateTimeUtils.millisToNanos(millis);
        return this;
    }

    @Override
    public String toString() {
        return "TestClock{" + DateTimeUtils.epochNanosToInstant(now).atZone(ZoneId.systemDefault()) + "}";
    }
}
