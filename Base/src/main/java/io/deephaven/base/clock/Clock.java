/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.clock;

import java.time.Instant;

/**
 * Provides epoch-based timestamps.
 */
public interface Clock {

    /**
     * Milliseconds since the epoch, 1970-01-01T00:00:00Z.
     *
     * @return epoch millis
     */
    long currentTimeMillis();

    /**
     * Microseconds since the epoch, 1970-01-01T00:00:00Z.
     *
     * @return epoch microseconds
     */
    long currentTimeMicros();

    /**
     * Nanoseconds since the epoch, 1970-01-01T00:00:00Z.
     *
     * <p>
     * Note: this value will overflow in the year 2292.
     *
     * @return epoch nanoseconds
     */
    long currentTimeNanos();

    /**
     * The instant.
     *
     * @return the instant
     */
    Instant currentTimeInstant();
}
