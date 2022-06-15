/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.clock;

import io.deephaven.base.clock.Clock;

public class LocalClockImpl implements Clock {

    public long currentTimeMillis() {
        return MicroTimer.clockRealtime() / 1000000;
    }

    public long currentTimeMicros() {
        return MicroTimer.clockRealtime() / 1000;
    }

    public long currentTimeNanos() {
        return MicroTimer.clockRealtime();
    }
}
