package io.deephaven.time;

import io.deephaven.base.clock.Clock;

import java.time.Instant;
import java.util.Objects;

public final class TimeProviderClock implements TimeProvider {
    private final Clock clock;

    public TimeProviderClock(Clock clock) {
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public long currentTimeMillis() {
        return clock.currentTimeMillis();
    }

    @Override
    public long currentTimeMicros() {
        return clock.currentTimeMicros();
    }

    @Override
    public long currentTimeNanos() {
        return clock.currentTimeNanos();
    }

    @Override
    public Instant currentTimeInstant() {
        return clock.currentTimeInstant();
    }

    @Override
    public long nanoTime() {
        return clock.nanoTime();
    }

    @Override
    public DateTime currentTime() {
        return new DateTime(clock.currentTimeNanos());
    }
}
