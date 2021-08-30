package io.deephaven.stats;

import io.deephaven.base.clock.Clock;

/**
 * A Deephaven-side shim for {@link io.deephaven.stats.StatsDriver}.
 */
public class Driver {
    public static void start(Clock clock, StatsIntradayLogger intraday, boolean getFdStats) {
        new StatsDriver(clock, intraday, getFdStats);
        new GcEventStats().install();
    }
}
