//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stats;

import io.deephaven.base.clock.Clock;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

/**
 * A Deephaven-side shim for {@link io.deephaven.stats.StatsDriver}.
 */
public class Driver {
    public static void start(Clock clock, StatsIntradayLogger intraday, boolean getFdStats) {
        final StatsDriver statsDriver = new StatsDriver(clock, intraday, getFdStats);
        final ShutdownManager shutdownManager = ProcessEnvironment.getGlobalShutdownManager();
        // TODO(DH-21651): Improve shutdown ordering constraints
        // Expressing in what appears to be "out-of-order", because within each category, there is a stack-based
        // execution (first-in, last-out); and we could theoretically change this first task to FIRST and it would need
        // to be in this order.
        shutdownManager.registerTask(ShutdownManager.OrderingCategory.MIDDLE, () -> {
            try (final StatsDriver _ignore = statsDriver) {
                statsDriver.awaitSchedulerTermination(Duration.ofMillis(100));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        shutdownManager.registerTask(ShutdownManager.OrderingCategory.FIRST, statsDriver::shutdownScheduler);
        new GcEventStats().install();
    }
}
