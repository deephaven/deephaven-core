package io.deephaven.db.util.liveness;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.Scheduler;
import io.deephaven.io.sched.TimedJob;
import io.deephaven.db.v2.DynamicNode;
import io.deephaven.util.HeapDump;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

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
            Configuration.getInstance().getBooleanWithDefault("Liveness.cleanupLogEnabled", true);

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
     * Note that this should be guarded by the LTM lock or similar.
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

    public static void scheduleCountReport(@NotNull final Scheduler scheduler) {
        scheduler.installJob(new TimedJob() {
            @Override
            public final void timedOut() {
                maybeLogOutstandingCount();
                scheduler.installJob(this, scheduler.currentTimeMillis() + OUTSTANDING_COUNT_LOG_INTERVAL_MILLIS);
            }
        }, 0L);
    }

    private Liveness() {}

    /**
     * <p>
     * Determine whether a cached object should be reused, w.r.t. liveness. Null inputs are never safe for reuse. If the
     * object is a {@link LivenessReferent} and not a non-refreshing {@link DynamicNode}, this method will return the
     * result of trying to manage object with the top of the current thread's {@link LivenessScopeStack}.
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
