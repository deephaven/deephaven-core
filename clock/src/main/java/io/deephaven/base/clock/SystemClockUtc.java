/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.clock;

import java.time.Instant;

/**
 * A clock based off of the current system time.
 *
 * @see java.time.Clock#systemUTC()
 */
public enum SystemClockUtc implements SystemClock {
    INSTANCE;

    /**
     * {@inheritDoc}
     *
     * <p>
     * Equivalent to {@link System#currentTimeMillis()}. This is the same source as {@link java.time.Clock#systemUTC()}.
     *
     * @return the current system time in epoch millis
     */
    @Override
    public long currentTimeMillis() {
        // See note in java.time.Clock.SystemClock#millis, this is the same source as java.time.Clock.systemUTC()
        return System.currentTimeMillis();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Calculates the time from {@link java.time.Clock#systemUTC()}. If you don't need need microsecond resolution,
     * prefer {@link #currentTimeMillis()}.
     *
     * <p>
     * Note: this method may allocate.
     */
    @Override
    public long currentTimeMicros() {
        final Instant now = java.time.Clock.systemUTC().instant();
        return now.getEpochSecond() * 1_000_000 + now.getNano() / 1_000;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Calculates the time from {@link java.time.Clock#systemUTC()}. If you don't need nanosecond resolution, prefer
     * {@link #currentTimeMillis()}.
     *
     * <p>
     * Note: this method may allocate.
     */
    @Override
    public long currentTimeNanos() {
        final Instant now = java.time.Clock.systemUTC().instant();
        return now.getEpochSecond() * 1_000_000_000 + now.getNano();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Equivalent to {@code java.time.Clock.systemUTC().instant()}.
     */
    @Override
    public Instant instantNanos() {
        return java.time.Clock.systemUTC().instant();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Equivalent to {@code Instant.ofEpochMilli(System.currentTimeMillis())}.
     */
    @Override
    public Instant instantMillis() {
        // See note in java.time.Clock.SystemClock#millis, this is the same source as java.time.Clock.systemUTC()
        return Instant.ofEpochMilli(System.currentTimeMillis());
    }
}
