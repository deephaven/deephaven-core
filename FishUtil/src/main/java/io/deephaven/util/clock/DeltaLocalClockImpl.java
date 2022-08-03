/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.clock;

import io.deephaven.base.clock.Clock;

import java.io.Serializable;

/**
 * Real-time local clock impl with a delta adjustment for all time values.
 */
public class DeltaLocalClockImpl implements Clock, Serializable {

    public static final long MINUS_ONE_DAY = -86400000000000L;

    private final long deltaNanos;

    public DeltaLocalClockImpl(final long deltaNanos) {
        this.deltaNanos = deltaNanos;
    }

    public long currentTimeMillis() {
        return (MicroTimer.clockRealtime() + deltaNanos) / 1000000;
    }

    public long currentTimeMicros() {
        return (MicroTimer.clockRealtime() + deltaNanos) / 1000;
    }

    public long currentTimeNanos() {
        return MicroTimer.clockRealtime() + deltaNanos;
    }
}
