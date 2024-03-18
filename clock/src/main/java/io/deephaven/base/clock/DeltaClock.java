//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.clock;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A clock impl with a delta adjustment.
 */
public final class DeltaClock implements Clock {

    /**
     * Constructs a delta clock with a {@link Clock#system()} clock.
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
     * Constructs a delta clock with a {@link Clock#system()} clock.
     *
     * @param deltaNanos the delta nanos
     */
    public DeltaClock(final long deltaNanos) {
        this(Clock.system(), deltaNanos);
    }

    /**
     * Constructs a delta clock with the provided clock and delta.
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
        return Math.addExact(delegate.currentTimeMillis(), deltaNanos / 1_000_000);
    }

    @Override
    public long currentTimeMicros() {
        return Math.addExact(delegate.currentTimeMicros(), deltaNanos / 1_000);
    }

    @Override
    public long currentTimeNanos() {
        return Math.addExact(delegate.currentTimeNanos(), deltaNanos);
    }

    @Override
    public Instant instantNanos() {
        return delegate.instantNanos().plusNanos(deltaNanos);
    }

    @Override
    public Instant instantMillis() {
        return delegate.instantMillis().plusNanos(deltaNanos);
    }
}
