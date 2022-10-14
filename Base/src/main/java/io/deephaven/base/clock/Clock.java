/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.clock;

import java.time.Instant;

/**
 * Provides time methods.
 */
public interface Clock {

    /**
     * The system clock.
     *
     * @return the system clock
     * @see SystemClock
     */
    static Clock systemUTC() {
        return SystemClock.INSTANCE;
    }

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

    /**
     * Provides a nanosecond timer for measuring elapsed time. This may not be related to any notion of system or
     * wall-clock time, so the results should only be compared with results from like-calls.
     *
     * <p>
     * For example, to measure how long some code takes to execute:
     * 
     * <pre>
     * long startNanoTime = clock.nanoTime();
     * // ... the code being measured ...
     * long elapsedNanos = clock.nanoTime() - startNanoTime;
     * </pre>
     * 
     * @return the nano time
     */
    long nanoTime();
}
