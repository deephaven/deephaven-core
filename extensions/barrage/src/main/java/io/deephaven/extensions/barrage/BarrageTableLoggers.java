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
 * Install the factory early in startup, but do not force {@link BarragePerformanceLog#getInstance()} at that point to
 * prove the ordering: constructing that singleton registers blink tables with the update graph of the current
 * {@link io.deephaven.engine.context.ExecutionContext}, which typically does not exist yet while startup code is
 * running. Let the first barrage activity create it, and rely on the warning that {@link #set(Factory)} logs if the
 * installation turns out to be too late.
 * <p>
 * This class is the template for extension modules that want pluggable table logging: keep the sink interfaces in the
 * module that owns them, expose a {@code <Module>TableLoggers} holder with a nested {@code Factory} and a {@code Noop}
 * default, and let the integrator install a factory during startup. Extension-specific loggers should not be added to
 * {@code io.deephaven.engine.tablelogger.EngineTableLoggers}, which is reserved for engine concepts. The ordering
 * caveat above generalizes as well: every such holder has its own "install before first use of singleton X" deadline,
 * and X's construction may need runtime machinery that startup code cannot supply.
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

    /**
     * Supplies the sinks that {@link BarragePerformanceLog} forwards its entries to. Each method is called exactly
     * once, when that singleton is constructed, on whichever thread first uses barrage.
     * <p>
     * Implementations should not throw: a sink that cannot be created should be reported and replaced with the
     * corresponding {@code Noop}, so that a defective recording path degrades barrage performance logging rather than
     * barrage itself. {@link BarragePerformanceLog} substitutes {@code Noop} for an accessor that throws or returns
     * null, but an implementation that handles its own failure can log something far more useful about the cause.
     */
    public interface Factory {
        /**
         * @return the sink to forward subscription performance entries to; never null
         */
        BarrageSubscriptionPerformanceSink subscriptionPerformanceSink();

        /**
         * @return the sink to forward snapshot performance entries to; never null
         */
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
