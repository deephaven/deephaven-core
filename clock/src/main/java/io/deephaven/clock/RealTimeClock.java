/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.clock;

import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

public interface RealTimeClock {
    static RealTimeClock loadImpl() {
        return RealTimeClockLoader.loadImpl();
    }

    default long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    default long currentTimeMicros() {
        return currentTimeNanos() / 1000L;
    }
    long currentTimeNanos();
}
