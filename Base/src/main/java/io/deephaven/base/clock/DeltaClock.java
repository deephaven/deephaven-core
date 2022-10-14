/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.clock;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A clock impl with a delta adjustment.
 */
public final class DeltaClock implements Clock {

    /**
     * Constructs a clock delta with a {@link Clock#systemUTC()} clock.
     *
     * @param duration the duration
     * @return the clock
     */
    public static DeltaClock of(Duration duration) {
        return new DeltaClock(duration.toNanos());
    }

    private final Clock delegate;
    private final long deltaNanos;

    /**
     * Constructs a clock delta with a {@link Clock#systemUTC()} clock.
     *
     * @param deltaNanos the delta nanos
     */
    public DeltaClock(final long deltaNanos) {
        this(Clock.systemUTC(), deltaNanos);
    }

    /**
     * Constructs a clock delta with the provided clock and delta.
     *
     * @param delegate the base clock
     * @param deltaNanos the delta nanos
     */
    public DeltaClock(Clock delegate, long deltaNanos) {
        this.delegate = Objects.requireNonNull(delegate);
        this.deltaNanos = deltaNanos;
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis() + deltaNanos / 1_000_000;
    }

    @Override
    public long currentTimeMicros() {
        return delegate.currentTimeMicros() + deltaNanos / 1_000;
    }

    @Override
    public long currentTimeNanos() {
        return delegate.currentTimeNanos() + deltaNanos;
    }

    @Override
    public Instant currentTimeInstant() {
        return delegate.currentTimeInstant().plusNanos(deltaNanos);
    }

    @Override
    public long nanoTime() {
        // Note: nanoTime() should only be used for relative durations; we don't need to apply delta.
        return delegate.nanoTime();
    }
}
