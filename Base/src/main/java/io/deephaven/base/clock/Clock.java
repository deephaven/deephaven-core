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
     * <p>
     * The resolution is dependent on the JVM and underlying implementation.
     *
     * @return epoch millis
     */
    long currentTimeMillis();

    /**
     * Microseconds since the epoch, 1970-01-01T00:00:00Z.
     *
     * <p>
     * The resolution is dependent on the JVM and underlying implementation. The resolution is greater than or equal to
     * {@link #currentTimeMillis()}.
     *
     * <p>
     * If you don't need microsecond resolution, prefer {@link #currentTimeMillis()}.
     *
     * @return epoch microseconds
     */
    long currentTimeMicros();

    /**
     * Nanoseconds since the epoch, 1970-01-01T00:00:00Z.
     *
     * <p>
     * The resolution is dependent on the JVM and underlying implementation. The resolution is greater than or equal to
     * {@link #currentTimeMicros()} and {@link #currentTimeMillis()}.
     *
     * <p>
     * If you don't need nanosecond resolution, prefer {@link #currentTimeMillis()}.
     *
     * @return epoch nanoseconds
     */
    long currentTimeNanos();

    /**
     * The instant.
     *
     * <p>
     * Has resolution equal to {@link #currentTimeNanos()}.
     *
     * @return the instant
     */
    Instant instantNanos();

    /**
     * The instant.
     *
     * <p>
     * Has resolution greater than or equal to {@link #currentTimeMillis()}.
     *
     * @return the instant.
     */
    Instant instantMillis();

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
     * <p>
     * The resolution is dependent on the JVM and underlying implementation.
     *
     * @return the nano time
     */
    long nanoTime();
}
