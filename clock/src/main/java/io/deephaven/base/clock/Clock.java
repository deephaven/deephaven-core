/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.clock;

import java.time.Instant;

/**
 * Provides time-based methods. Callers should prefer the methods most appropriate for their use-case and desired
 * resolution. The performance of the methods may differ depending on the resolution they provide.
 *
 * @see SystemClock
 */
public interface Clock {

    /**
     * The {@link SystemClock}. Provides singleton semantics around {@link SystemClock#of()}.
     *
     * @return the system clock
     */
    static SystemClock system() {
        return SystemClockInstance.INSTANCE;
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
     * @return epoch nanoseconds
     */
    long currentTimeNanos();

    /**
     * The instant.
     *
     * <p>
     * Has resolution equal to {@link #currentTimeNanos()}.
     *
     * <p>
     * If you don't need the resolution provided by {@link #currentTimeNanos()}, prefer {@link #instantMillis()}.
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
}
