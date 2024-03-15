//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.hotspot;

/**
 * Utility class to facilitate obtaining data for safepoint pauses count and time between two points in code. A
 * safepoint pause is a "stop the world, pause all threads" event in the HotSpot JVM. Note full Garbage Collection
 * pauses are a dominant cause of safepoint pauses, but there are other triggers like:
 * <ol>
 * <li>Deoptimization</li>
 * <li>Biased lock revocation</li>
 * <li>Thread dump</li>
 * <li>Heap inspection</li>
 * <li>Class redefinition</li>
 * </ol>
 * And others; you can see a full list <a href=
 * "https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/19fb8f93c59dfd791f62d41f332db9e306bc1422/src/hotspot/share/runtime/vm_operations.hpp#L40">
 * here </a>.
 *
 */

@SuppressWarnings("restriction")
public class JvmIntrospectionContext {
    private final HotSpot hotspot;
    private long lastStartPausesCount = -1;
    private long lastStartPausesTimeMillis = -1;
    private long lastStartSyncTimeMillis = -1;
    private long lastEndPausesCount;
    private long lastEndPausesTimeMillis;
    private long lastEndSyncTimeMillis;

    public boolean hasSafePointData() {
        return hotspot != null;
    }

    public JvmIntrospectionContext() {
        hotspot = HotSpot.loadImpl().orElse(null);
    }

    public void startSample() {
        if (hotspot == null) {
            return;
        }
        lastStartPausesCount = hotspot.getSafepointCount();
        lastStartPausesTimeMillis = hotspot.getTotalSafepointTimeMillis();
        lastStartSyncTimeMillis = hotspot.getSafepointSyncTimeMillis();
    }

    /**
     * Sample garbage collection count and times at the point of call.
     */
    public void endSample() {
        if (hotspot == null) {
            return;
        }
        lastEndPausesCount = hotspot.getSafepointCount();
        lastEndPausesTimeMillis = hotspot.getTotalSafepointTimeMillis();
        lastEndSyncTimeMillis = hotspot.getSafepointSyncTimeMillis();
    }

    /**
     * Number of safepoint pauses between the last two calls to {@code sample()}
     * 
     * @return Number of safepoint pauses.
     */
    public long deltaSafePointPausesCount() {
        return lastEndPausesCount - lastStartPausesCount;
    }

    /**
     * Time in milliseconds fully paused in safepoints between the last two calls to {@code sample()}
     * 
     * @return Time in milliseconds.
     */
    public long deltaSafePointPausesTimeMillis() {
        return lastEndPausesTimeMillis - lastStartPausesTimeMillis;
    }

    /**
     * Time in milliseconds getting to a full safepoint stop (safepoint sync time) between the last two calls to
     * {@code sample()}
     *
     * @return Time in milliseconds
     */
    public long deltaSafePointSyncTimeMillis() {
        return lastEndSyncTimeMillis - lastStartSyncTimeMillis;
    }

}
