//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.util.HeapDump;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for liveness-related instrumentation.
 */
public final class Liveness {

    public static final Logger log = LoggerFactory.getLogger(Liveness.class);

    static final boolean REFERENCE_TRACKING_DISABLED =
            Configuration.getInstance().getBooleanWithDefault("Liveness.referenceTrackingDisabled", false);

    public static final boolean DEBUG_MODE_ENABLED =
            Configuration.getInstance().getBooleanWithDefault("Liveness.debugModeEnabled", false);

    private static final boolean COUNT_LOG_ENABLED =
            Configuration.getInstance().getBooleanWithDefault("Liveness.countLogEnabled", true);

    private static final boolean HEAP_DUMP_ENABLED =
            Configuration.getInstance().getBooleanWithDefault("Liveness.heapDump", false);

    static final boolean CLEANUP_LOG_ENABLED =
            Configuration.getInstance().getBooleanWithDefault("Liveness.cleanupLogEnabled", false);

    private static final long OUTSTANDING_COUNT_LOG_INTERVAL_MILLIS = 1000L;

    private static boolean outstandingCountChanged = true;
    private static int intervalLastOutstandingCount = 0;
    private static int intervalMinOutstandingCount = 0;
    private static int intervalMaxOutstandingCount = 0;

    /**
     * <p>
     * Maybe log the count of known outstanding {@link LivenessReferent}s.
     * <p>
     * Will not log unless such logs are enabled, at least {@value OUTSTANDING_COUNT_LOG_INTERVAL_MILLIS}ms have
     * elapsed, and the count has changed since the last time it was logged.
     * <p>
     * Note that this should be guarded by the UGP lock or similar.
     */
    private static void maybeLogOutstandingCount() {
        if (!Liveness.COUNT_LOG_ENABLED) {
            return;
        }

        final int outstandingCount = RetainedReferenceTracker.getOutstandingCount();
        if (outstandingCount != intervalLastOutstandingCount) {
            outstandingCountChanged = true;
        }

        intervalMinOutstandingCount = Math.min(outstandingCount, intervalMinOutstandingCount);
        intervalMaxOutstandingCount = Math.max(outstandingCount, intervalMaxOutstandingCount);

        if (!outstandingCountChanged) {
            return;
        }

        Liveness.log.info().append("Liveness: Outstanding count=").append(outstandingCount)
                .append(", intervalMin=").append(intervalMinOutstandingCount)
                .append(", intervalMax=").append(intervalMaxOutstandingCount)
                .endl();
        outstandingCountChanged = false;
        intervalLastOutstandingCount = intervalMinOutstandingCount = intervalMaxOutstandingCount = outstandingCount;
    }

    /**
     * Schedule a job to log the count of known outstanding {@link LivenessReferent LivenessReferents}.
     *
     * @param scheduler The {@link ScheduledExecutorService} to use
     * @return The {@link ScheduledFuture} for the scheduled job
     */
    public static ScheduledFuture<?> scheduleCountReport(@NotNull final ScheduledExecutorService scheduler) {
        return scheduler.scheduleAtFixedRate(
                Liveness::maybeLogOutstandingCount,
                0L,
                OUTSTANDING_COUNT_LOG_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private Liveness() {}

    /**
     * <p>
     * Determine whether a cached object should be reused, w.r.t. liveness. Null inputs are never safe for reuse. If the
     * object is a {@link LivenessReferent} and is a refreshing {@link DynamicNode}, this method will return the result
     * of trying to manage object with the top of the current thread's {@link LivenessScopeStack}.
     *
     * @param object The object
     * @return True if the object did not need management, or if it was successfully managed, false otherwise
     */
    public static boolean verifyCachedObjectForReuse(final Object object) {
        if (object == null) {
            return false;
        }
        if (!(object instanceof LivenessReferent)) {
            return true;
        }
        if (DynamicNode.isDynamicAndNotRefreshing(object)) {
            return true;
        }
        return LivenessScopeStack.peek().tryManage((LivenessReferent) object);
    }

    static void maybeHeapDump(LivenessStateException lse) {
        if (!HEAP_DUMP_ENABLED) {
            return;
        }
        final String heapDumpPath = HeapDump.generateHeapDumpPath();
        log.fatal().append("LivenessStateException, generating heap dump to").append(heapDumpPath).append(": ")
                .append(lse).endl();
        try {
            HeapDump.heapDump(heapDumpPath);
        } catch (IOException ignored) {
        }
    }
}
