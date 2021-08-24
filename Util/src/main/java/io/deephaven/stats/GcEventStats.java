package io.deephaven.stats;

import static com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION;

import io.deephaven.base.stats.Counter;
import io.deephaven.base.stats.Stats;
import com.sun.management.GarbageCollectionNotificationInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

/**
 * Gc event stats provides the stats {@code Memory-GC.Reclaimed} and {@code Memory.Allocated}.
 *
 * <p>
 * Unfortunately, the JVM doesn't seem to natively provide access to how much data has been
 * allocated, so we are using the GC before and after values as a proxy for that. (The JVM *does*
 * provide it on a per-thread basis though,
 * {@link com.sun.management.ThreadMXBean#getThreadAllocatedBytes(long)}.)
 *
 * <p>
 * This implementation relies on
 * {@link GarbageCollectionNotificationInfo#GARBAGE_COLLECTION_NOTIFICATION} to get stats around the
 * memory usage before and after GCs. This means that these statistics are somewhat less useful when
 * GC events are sparse.
 */
class GcEventStats implements NotificationListener {

    private final Counter reclaimed;
    private final Counter allocated;
    private long lastAfterGc = 0;

    GcEventStats() {
        reclaimed = Stats.makeItem("Memory-GC", "Reclaimed", Counter.FACTORY).getValue();
        allocated = Stats.makeItem("Memory", "Allocated", Counter.FACTORY).getValue();
    }

    public void install() {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).addNotificationListener(this, null, null);
        }
    }

    public void remove() throws ListenerNotFoundException {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).removeNotificationListener(this);
        }
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
        if (!GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
            return;
        }
        handleGCInfo(
            GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData()));
    }

    // synchronized *shouldn't* be necessary, but doesn't hurt.
    private synchronized void handleGCInfo(GarbageCollectionNotificationInfo info) {
        final long beforeGc = info.getGcInfo().getMemoryUsageBeforeGc().values().stream()
            .mapToLong(MemoryUsage::getUsed).sum();
        final long afterGc = info.getGcInfo().getMemoryUsageAfterGc().values().stream()
            .mapToLong(MemoryUsage::getUsed).sum();
        final long reclaimedBytes = beforeGc - afterGc; // note: this *CAN* be negative
        reclaimed.increment(reclaimedBytes);
        // There are potentially other interesting stats that can be derived here in the future. The
        // stats could be broken down by pool name / gc type.
        final long allocatedBytes = beforeGc - lastAfterGc;
        allocated.increment(allocatedBytes);
        lastAfterGc = afterGc;
    }
}
