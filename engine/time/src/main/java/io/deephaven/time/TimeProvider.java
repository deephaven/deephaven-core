/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.clock.Clock;

/**
 * Interface for providing the current time.
 */
public interface TimeProvider extends Clock {

    /**
     * The system time provider.
     *
     * @return the system time provider
     * @see SystemTimeProvider
     */
    static TimeProvider systemUTC() {
        return SystemTimeProvider.INSTANCE;
    }

    DateTime currentTime();
}
