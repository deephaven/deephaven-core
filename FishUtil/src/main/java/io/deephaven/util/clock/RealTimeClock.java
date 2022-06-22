/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.clock;

import io.deephaven.base.clock.Clock;

/**
 * This is the simplest possible "real" clock implementation.
 */
public class RealTimeClock implements Clock {

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public long currentTimeMicros() {
        return MicroTimer.currentTimeMicros();
    }
}
