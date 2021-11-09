package io.deephaven.engine.v2.utils;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.tables.utils.DateTime;
import io.deephaven.engine.tables.utils.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Adapter to allow a Clock to serve as a TimeProvider.
 */
public class ClockTimeProvider implements TimeProvider {

    private final Clock clock;

    public ClockTimeProvider(@NotNull final Clock clock) {
        this.clock = Require.neqNull(clock, "clock");
    }

    @Override
    public DateTime currentTime() {
        // TODO: Consider adding last-value caching.
        return DateTimeUtils.microsToTime(clock.currentTimeMicros());
    }
}
