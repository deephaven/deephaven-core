package io.deephaven.base.clock;

public abstract class ClockNanoBase implements Clock {
    @Override
    public final long currentTimeMillis() {
        return currentTimeNanos() / 1_000_000;
    }

    @Override
    public final long currentTimeMicros() {
        return currentTimeNanos() / 1_000;
    }
}
