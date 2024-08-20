//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.app;

import com.sun.management.GarbageCollectionNotificationInfo;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.SafeCloseable;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import static com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION;

/**
 * The {@value APP_NAME}, application id {@value APP_ID}, produces stream {@link io.deephaven.engine.table.Table tables}
 * {@value NOTIFICATION_INFO} and {@value POOLS}; and derived tables {@value NOTIFICATION_INFO_STATS},
 * {@value NOTIFICATION_INFO_RING}, and {@value POOLS_STATS}. This data is modeled after the
 * {@link GarbageCollectionNotificationInfo} event information from
 * {@link ManagementFactory#getGarbageCollectorMXBeans()}.
 *
 * @see #enabled()
 * @see #notificationInfoEnabled()
 * @see #poolsEnabled()
 * @see #notificationInfoStatsEnabled()
 * @see #notificationInfoRingSize()
 * @see #poolStatsEnabled()
 */
public final class GcApplication implements ApplicationState.Factory, NotificationListener {

    private static final String APP_ID = "io.deephaven.app.GcApplication";
    private static final String APP_NAME = "Garbage-Collection Application";
    private static final String NOTIFICATION_INFO = "notification_info";
    private static final String NOTIFICATION_INFO_STATS = "notification_info_stats";
    private static final String NOTIFICATION_INFO_RING = "notification_info_ring";
    private static final String POOLS = "pools";
    private static final String POOLS_STATS = "pools_stats";

    private static final String ENABLED = "io.deephaven.app.GcApplication.enabled";
    private static final String NOTIFICATION_INFO_ENABLED = "io.deephaven.app.GcApplication.notification_info.enabled";
    private static final String NOTIFICATION_INFO_STATS_ENABLED =
            "io.deephaven.app.GcApplication.notification_info_stats.enabled";
    private static final String NOTIFICATION_INFO_RING_ENABLED =
            "io.deephaven.app.GcApplication.notification_info_ring.enabled";
    private static final String POOLS_ENABLED = "io.deephaven.app.GcApplication.pools.enabled";
    private static final String POOLS_STATS_ENABLED = "io.deephaven.app.GcApplication.pools_stats.enabled";

    /**
     * Looks up the system property {@value ENABLED}, defaults to {@code false}.
     *
     * @return if the GC application is enabled
     */
    public static boolean enabled() {
        // Note: this is off-by-default until the Web UI has been updated to better handle on-by-default applications.
        return "true".equalsIgnoreCase(System.getProperty(ENABLED));
    }

    /**
     * Looks up the system property {@value NOTIFICATION_INFO_ENABLED}, defaults to {@code true}.
     *
     * @return if {@value NOTIFICATION_INFO} table is enabled
     */
    public static boolean notificationInfoEnabled() {
        return "true".equalsIgnoreCase(System.getProperty(NOTIFICATION_INFO_ENABLED, "true"));
    }

    /**
     * Looks up the system property {@value NOTIFICATION_INFO_STATS_ENABLED}, defaults to {@code true}.
     *
     * @return if {@value NOTIFICATION_INFO_STATS} table is enabled
     */
    public static boolean notificationInfoStatsEnabled() {
        return "true".equalsIgnoreCase(System.getProperty(NOTIFICATION_INFO_STATS_ENABLED, "true"));
    }

    /**
     * Looks up the system property {@value NOTIFICATION_INFO_RING_ENABLED}, defaults to {@code 1024}. The
     * {@value NOTIFICATION_INFO_RING} table is disabled when {@code 0} or less.
     *
     * @return the {@value NOTIFICATION_INFO_RING} table size
     */
    public static int notificationInfoRingSize() {
        return Integer.getInteger(NOTIFICATION_INFO_RING_ENABLED, 1024);
    }

    /**
     * Looks up the system property {@value POOLS_ENABLED}, defaults to {@code true}.
     *
     * @return if {@value POOLS} table is enabled
     */
    public static boolean poolsEnabled() {
        return "true".equalsIgnoreCase(System.getProperty(POOLS_ENABLED, "true"));
    }

    /**
     * Looks up the system property {@value POOLS_STATS_ENABLED}, defaults to {@code true}.
     *
     * @return if {@value POOLS_STATS} table is enabled
     */
    public static boolean poolStatsEnabled() {
        return "true".equalsIgnoreCase(System.getProperty(POOLS_STATS_ENABLED, "true"));
    }

    private GcNotificationPublisher notificationInfoPublisher;
    private GcPoolsPublisher poolsPublisher;
    @SuppressWarnings("FieldCanBeLocal")
    private LivenessScope scope;

    @Override
    public void handleNotification(Notification notification, Object handback) {
        try {
            final GarbageCollectionNotificationInfo info =
                    GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
            notificationInfoPublisher.add(info);
            poolsPublisher.add(info.getGcInfo());
        } catch (Throwable t) {
            notificationInfoPublisher.acceptFailure(t);
            poolsPublisher.acceptFailure(t);
            try {
                remove();
            } catch (ListenerNotFoundException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    @Override
    public ApplicationState create(Listener listener) {
        final ApplicationState state = new ApplicationState(listener, APP_ID, APP_NAME);
        if (!enabled()) {
            return state;
        }
        final boolean notificationInfoEnabled = notificationInfoEnabled();
        final boolean poolsEnabled = poolsEnabled();
        if (!notificationInfoEnabled() && !poolsEnabled()) {
            return state;
        }
        scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            if (notificationInfoEnabled) {
                setNotificationInfo(state);
            }
            if (poolsEnabled) {
                setPools(state);
            }
        }
        install();
        return state;
    }

    private void setNotificationInfo(ApplicationState state) {
        notificationInfoPublisher = new GcNotificationPublisher();
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(GcNotificationPublisher.definition(),
                notificationInfoPublisher, ExecutionContext.getContext().getUpdateGraph(), NOTIFICATION_INFO);
        final Table notificationInfo = adapter.table();
        state.setField(NOTIFICATION_INFO, notificationInfo);
        if (notificationInfoStatsEnabled()) {
            state.setField(NOTIFICATION_INFO_STATS, GcNotificationPublisher.stats(notificationInfo));
        }
        final int ringSize = notificationInfoRingSize();
        if (ringSize > 0) {
            state.setField(NOTIFICATION_INFO_RING, RingTableTools.of(notificationInfo, ringSize));
        }
    }

    private void setPools(ApplicationState state) {
        poolsPublisher = new GcPoolsPublisher();
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(GcPoolsPublisher.definition(),
                poolsPublisher, ExecutionContext.getContext().getUpdateGraph(), POOLS);
        final Table pools = adapter.table();
        state.setField(POOLS, pools);
        if (poolStatsEnabled()) {
            state.setField(POOLS_STATS, GcPoolsPublisher.stats(pools));
        }
    }

    private void install() {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).addNotificationListener(this, GcNotificationFilter.INSTANCE, null);
        }
    }

    private void remove() throws ListenerNotFoundException {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).removeNotificationListener(this);
        }
    }

    private enum GcNotificationFilter implements NotificationFilter {
        INSTANCE;

        @Override
        public boolean isNotificationEnabled(Notification notification) {
            return GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType());
        }
    }
}
