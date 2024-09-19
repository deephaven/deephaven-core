//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.clock.Clock;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;
import java.time.ZoneId;

// TODO: MOVE THIS TO THE EXISTING TESTCLOCK

/**
 * A clock that has a fixed time for use in unit tests.
 */
public class TestClock implements Clock {
    private long nanos;

    /**
     * Set the time.
     * 
     * @param epochNanos the time to set in nanos since the epoch
     * @return this clock
     */
    public TestClock setNanos(long epochNanos) {
        this.nanos = epochNanos;
        return this;
    }

    /**
     * Set the time.
     * 
     * @param epochMillis the time to set in millis since the epoch
     * @return this clock
     */
    public TestClock setMillis(long epochMillis) {
        this.nanos = DateTimeUtils.millisToNanos(epochMillis);
        return this;
    }

    /**
     * Add millis to the current time.
     * 
     * @param millis the number of millis to add
     * @return this clock
     */
    public TestClock addMillis(long millis) {
        this.nanos += DateTimeUtils.millisToNanos(millis);
        return this;
    }

    @Override
    public long currentTimeMillis() {
        return DateTimeUtils.nanosToMillis(nanos);
    }

    @Override
    public long currentTimeMicros() {
        return DateTimeUtils.nanosToMicros(nanos);
    }

    @Override
    public long currentTimeNanos() {
        return nanos;
    }

    @Override
    public Instant instantNanos() {
        return DateTimeUtils.epochNanosToInstant(nanos);
    }

    @Override
    public Instant instantMillis() {
        return instantNanos();
    }

    @Override
    public String toString() {
        return "TestClock{" + DateTimeUtils.epochNanosToInstant(nanos).atZone(ZoneId.systemDefault()) + "}";
    }
}
