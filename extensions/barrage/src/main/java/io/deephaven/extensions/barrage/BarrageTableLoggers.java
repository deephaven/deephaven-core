//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.util.Objects;

/**
 * Provides the factory for barrage performance sinks, allowing an integrator to record barrage performance data in
 * addition to the in-memory tables exposed by {@link BarragePerformanceLog}.
 * <p>
 * By default nothing is recorded: {@link Factory.Noop} is installed and barrage performance data is available only from
 * the in-memory tables. An integrator installs its own factory with {@link #set(Factory)}, which must happen before the
 * first use of {@link BarragePerformanceLog}, because the sinks are resolved once, when that singleton is constructed.
 * <p>
 * This class is the template for extension modules that want pluggable table logging: keep the sink interfaces in the
 * module that owns them, expose a {@code <Module>TableLoggers} holder with a nested {@code Factory} and a {@code Noop}
 * default, and let the integrator install a factory during startup. Extension-specific loggers should not be added to
 * {@code io.deephaven.engine.tablelogger.EngineTableLoggers}, which is reserved for engine concepts.
 */
public class BarrageTableLoggers {
    private static final Logger log = LoggerFactory.getLogger(BarrageTableLoggers.class);

    private BarrageTableLoggers() {}

    private static volatile Factory factory = Factory.Noop.INSTANCE;

    /**
     * Whether {@link #get()} has already been called, and therefore whether a subsequent {@link #set(Factory)} is too
     * late to affect the sinks that have already been resolved.
     */
    private static volatile boolean resolved = false;

    public static Factory get() {
        resolved = true;
        return factory;
    }

    public static void set(final Factory factory) {
        if (resolved) {
            log.warn().append("BarrageTableLoggers.set called after the barrage performance sinks were already "
                    + "resolved; already-constructed loggers will continue to use the previously installed factory")
                    .endl();
        }
        BarrageTableLoggers.factory = Objects.requireNonNull(factory);
    }

    public interface Factory {
        BarrageSubscriptionPerformanceSink subscriptionPerformanceSink();

        BarrageSnapshotPerformanceSink snapshotPerformanceSink();

        enum Noop implements Factory {
            INSTANCE;

            @Override
            public BarrageSubscriptionPerformanceSink subscriptionPerformanceSink() {
                return BarrageSubscriptionPerformanceSink.Noop.INSTANCE;
            }

            @Override
            public BarrageSnapshotPerformanceSink snapshotPerformanceSink() {
                return BarrageSnapshotPerformanceSink.Noop.INSTANCE;
            }
        }
    }
}
