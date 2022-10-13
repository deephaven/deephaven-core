package io.deephaven.time;

import io.deephaven.base.clock.ClockNanoBase;

/**
 * A base implementation of TimeProvider, with all methods being sourced from {@link #currentTimeNanos()}.
 */
public abstract class TimeProviderNanoBase extends ClockNanoBase implements TimeProvider {

    @Override
    public final DateTime currentTime() {
        return new DateTime(currentTimeNanos());
    }
}
