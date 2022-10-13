/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.clock;

import io.deephaven.base.clock.Clock;

import java.time.Instant;

/**
 * This is the simplest possible "real" clock implementation.
 *
 * @see java.time.Clock#systemUTC()
 */
public enum RealTimeClock implements Clock {
    INSTANCE;

    @Override
    public long currentTimeMillis() {
        // See note in java.time.Clock.SystemClock#millis, this is the same source as java.time.Clock.systemUTC()
        return System.currentTimeMillis();
    }

    @Override
    public long currentTimeMicros() {
        final Instant now = java.time.Clock.systemUTC().instant();
        return now.getEpochSecond() * 1_000_000 + now.getNano() / 1_000;
    }

    @Override
    public long currentTimeNanos() {
        final Instant now = java.time.Clock.systemUTC().instant();
        return now.getEpochSecond() * 1_000_000_000 + now.getNano();
    }

    @Override
    public Instant currentTimeInstant() {
        return java.time.Clock.systemUTC().instant();
    }

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }
}
