/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

/**
 * Adapter to allow a Clock to serve as a TimeProvider.
 */
public final class ClockTimeProvider implements TimeProvider {

    private final Clock clock;

    public ClockTimeProvider(@NotNull final Clock clock) {
        this.clock = Require.neqNull(clock, "clock");
    }

    @Override
    public DateTime currentTime() {
        // TODO: Consider adding last-value caching.
        return new DateTime(clock.currentTimeNanos());
    }

    @Override
    public long currentTimeMillis() {
        return clock.currentTimeMillis();
    }

    @Override
    public long currentTimeMicros() {
        return clock.currentTimeMicros();
    }

    @Override
    public long currentTimeNanos() {
        return clock.currentTimeNanos();
    }
}
