package io.deephaven.time;

import io.deephaven.base.clock.Clock;

import java.time.Instant;

/**
 * The system time provider based on {@link Clock#systemUTC()}.
 */
public enum SystemTimeProvider implements TimeProvider {
    INSTANCE;

    @Override
    public long currentTimeMillis() {
        return Clock.systemUTC().currentTimeMillis();
    }

    @Override
    public long currentTimeMicros() {
        return Clock.systemUTC().currentTimeMicros();
    }

    @Override
    public long currentTimeNanos() {
        return Clock.systemUTC().currentTimeNanos();
    }

    @Override
    public Instant currentTimeInstant() {
        return Clock.systemUTC().currentTimeInstant();
    }

    @Override
    public long nanoTime() {
        return Clock.systemUTC().nanoTime();
    }

    @Override
    public DateTime currentTime() {
        return new DateTime(Clock.systemUTC().currentTimeNanos());
    }
}
