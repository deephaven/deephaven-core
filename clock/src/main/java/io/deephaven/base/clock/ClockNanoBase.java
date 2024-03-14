//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.clock;

import java.time.Instant;

/**
 * A base implementation of Clock, with all methods being sourced from {@link #currentTimeNanos()}.
 */
public abstract class ClockNanoBase implements Clock {
    @Override
    public final long currentTimeMillis() {
        return currentTimeNanos() / 1_000_000;
    }

    @Override
    public final long currentTimeMicros() {
        return currentTimeNanos() / 1_000;
    }

    @Override
    public final Instant instantNanos() {
        return Instant.ofEpochSecond(0, currentTimeNanos());
    }

    @Override
    public final Instant instantMillis() {
        return Instant.ofEpochSecond(0, currentTimeNanos());
    }
}
