/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.clock;

import java.time.Instant;

/**
 * A clock based off of {@link System#currentTimeMillis()}.
 *
 * <p>
 * This clock differs from {@link SystemClockUtc} in that all timestamp methods have millisecond-level resolution.
 */
public enum SystemClockMillis implements SystemClock {
    INSTANCE;

    /**
     * Equivalent to {@link System#currentTimeMillis()}.
     */
    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Equivalent to {@code System.currentTimeMillis() * 1_000}.
     */
    @Override
    public long currentTimeMicros() {
        return System.currentTimeMillis() * 1_000;
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
        return System.currentTimeMillis() * 1_000_000;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Equivalent to {@code Instant.ofEpochMilli(System.currentTimeMillis())}.
     */
    @Override
    public Instant instantNanos() {
        return Instant.ofEpochMilli(System.currentTimeMillis());
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Equivalent to {@code Instant.ofEpochMilli(System.currentTimeMillis())}.
     */
    @Override
    public Instant instantMillis() {
        return Instant.ofEpochMilli(System.currentTimeMillis());
    }
}
