package io.deephaven.time;

import io.deephaven.base.clock.ClockNanoBase;

public abstract class TimeProviderNanoBase extends ClockNanoBase implements TimeProvider {

    @Override
    public final DateTime currentTime() {
        return new DateTime(currentTimeNanos());
    }
}
