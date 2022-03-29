package io.deephaven.hotspot;

/**
 * Utility class to facilitate obtaining data for safepoint pauses count and time between two points in code. A
 * safepoint pause is a "stop the world, pause all threads" event in the HotSpot JVM. Note full Garbage Collection
 * pauses are a dominant cause of safepoint pauses, but there are other triggers like: <il>
 * <li>Deoptimization</li>
 * <li>Biased lock revocation</li>
 * <li>Thread dump</li>
 * <li>Heap inspection< /li>
 * <li>Class redefinition</li> </il> And others; you can see a full list <a href=
 * "http://hg.openjdk.java.net/jdk8u/jdk8u/hotspot/file/fc3cd1db10e2/src/share/vm/runtime/vm_operations.hpp#l39"> here
 * </a>.
 *
 */

@SuppressWarnings("restriction")
public class JvmIntrospectionContext {
    private final HotSpot hotspot;
    private long lastStartPausesCount = -1;
    private long lastStartPausesTimeMillis = -1;
    private long lastEndPausesCount;
    private long lastEndPausesTimeMillis;

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
    }

    /**
     * Number of collections between the last two calls to {@code sample()}
     * 
     * @return Number of collections
     */
    public long deltaSafePointPausesCount() {
        return lastEndPausesCount - lastStartPausesCount;
    }

    /**
     * Time in milliseconds in collections between the last two calls to {@code sample()}
     * 
     * @return Time in milliseconds
     */
    public long deltaSafePointPausesTimeMillis() {
        return lastEndPausesTimeMillis - lastStartPausesTimeMillis;
    }
}
