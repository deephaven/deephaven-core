//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stats;

import io.deephaven.base.clock.Clock;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

/**
 * A Deephaven-side shim for {@link io.deephaven.stats.StatsDriver}.
 */
public class Driver {
    private static final long SHUTDOWN_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);

    public static void start(Clock clock, StatsIntradayLogger intraday, boolean getFdStats) {
        final StatsDriver statsDriver = new StatsDriver(clock, intraday, getFdStats);
        final ShutdownManager shutdownManager = ProcessEnvironment.getGlobalShutdownManager();
        // TODO(DH-21651): Improve shutdown ordering constraints
        final long[] deadline = new long[1];
        shutdownManager.registerTask(ShutdownManager.OrderingCategory.FIRST, () -> {
            deadline[0] = System.nanoTime() + SHUTDOWN_TIMEOUT_NANOS;
            statsDriver.shutdownScheduler();
        });
        shutdownManager.registerTask(ShutdownManager.OrderingCategory.MIDDLE, () -> {
            final long remainingNanos = deadline[0] - System.nanoTime();
            try (final StatsDriver ignored = statsDriver) {
                statsDriver.awaitSchedulerTermination(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        new GcEventStats().install();
    }
}
