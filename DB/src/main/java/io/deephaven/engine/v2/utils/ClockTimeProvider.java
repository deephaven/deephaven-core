package io.deephaven.engine.v2.utils;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;
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
    public DBDateTime currentTime() {
        // TODO: Consider adding last-value caching.
        return DBTimeUtils.microsToTime(clock.currentTimeMicros());
    }
}
