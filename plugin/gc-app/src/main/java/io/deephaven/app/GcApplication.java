package io.deephaven.app;

import com.google.auto.service.AutoService;
import com.sun.management.GarbageCollectionNotificationInfo;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.stream.SetableStreamPublisher;
import io.deephaven.stream.StreamToTableAdapter;
import io.deephaven.util.SafeCloseable;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import static com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION;

/**
 * The Garbage-Collection Application produces stream {@link io.deephaven.engine.table.Table tables} "notification_info"
 * and "pools"; and derived tables "notification_info_stats", "notification_info_ring", and "pools_stats". This data is
 * modeled after the {@link GarbageCollectionNotificationInfo} event information from
 * {@link ManagementFactory#getGarbageCollectorMXBeans()}.
 *
 * @see #enabled()
 * @see #notificationInfoEnabled()
 * @see #poolsEnabled()
 * @see #notificationInfoStatsEnabled()
 * @see #notificationInfoRingSize()
 * @see #poolStatsEnabled()
 */
@AutoService(ApplicationState.Factory.class)
public final class GcApplication implements ApplicationState.Factory, NotificationListener {

    /**
     * Looks up the system property "io.deephaven.app.GcApplication.enabled", defaults to {@code false}.
     *
     * @return if the GC application is enabled
     */
    private static boolean enabled() {
        // Note: this is off-by-default until the Web UI has been updated to better handle on-by-default applications.
        return "true".equalsIgnoreCase(System.getProperty(String.format("%s.enabled", GcApplication.class.getName())));
    }

    /**
     * Looks up the system property "io.deephaven.app.GcApplication.notification_info.enabled", defaults to
     * {@code true}.
     *
     * @return if notification_info table is enabled
     */
    public static boolean notificationInfoEnabled() {
        return "true".equalsIgnoreCase(System
                .getProperty(String.format("%s.notification_info.enabled", GcApplication.class.getName()), "true"));
    }

    /**
     * Looks up the system property "io.deephaven.app.GcApplication.notification_info_stats.enabled", defaults to
     * {@code true}.
     *
     * @return if notification_info_stats table is enabled
     */
    public static boolean notificationInfoStatsEnabled() {
        return "true".equalsIgnoreCase(System.getProperty(
                String.format("%s.notification_info_stats.enabled", GcApplication.class.getName()), "true"));
    }

    /**
     * Looks up the system property "io.deephaven.app.GcApplication.notification_info_ring.size", defaults to
     * {@code 1024}. The notification_info_ring table is disable when {@code 0} or less.
     *
     * @return the notification_info_ring table size
     */
    public static int notificationInfoRingSize() {
        return Integer.getInteger(String.format("%s.notification_info_ring.size", GcApplication.class.getName()), 1024);
    }

    /**
     * Looks up the system property "io.deephaven.app.GcApplication.pools.enabled", defaults to {@code true}.
     *
     * @return if pools table is enabled
     */
    public static boolean poolsEnabled() {
        return "true".equalsIgnoreCase(
                System.getProperty(String.format("%s.pools.enabled", GcApplication.class.getName()), "true"));
    }

    /**
     * Looks up the system property "io.deephaven.app.GcApplication.pools_stats.enabled", defaults to {@code true}.
     *
     * @return if pools_stats table is enabled
     */
    public static boolean poolStatsEnabled() {
        return "true".equalsIgnoreCase(
                System.getProperty(String.format("%s.pools_stats.enabled", GcApplication.class.getName()), "true"));
    }

    @SuppressWarnings("FieldCanBeLocal")
    private LivenessScope scope;
    private GcNotificationConsumer notificationInfoConsumer;
    private GcPoolsConsumer poolsConsumer;

    @Override
    public void handleNotification(Notification notification, Object handback) {
        if (!GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
            return;
        }
        try {
            final GarbageCollectionNotificationInfo info =
                    GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
            notificationInfoConsumer.add(info);
            poolsConsumer.add(info.getGcInfo());
        } catch (Throwable t) {
            notificationInfoConsumer.acceptFailure(t);
            poolsConsumer.acceptFailure(t);
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
        final ApplicationState state =
                new ApplicationState(listener, GcApplication.class.getName(), "Garbage-Collection Application");
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
        final String name = "notification_info";
        final SetableStreamPublisher publisher = new SetableStreamPublisher();
        // noinspection resource
        final StreamToTableAdapter adapter = new StreamToTableAdapter(GcNotificationConsumer.definition(),
                publisher, UpdateGraphProcessor.DEFAULT, name);
        notificationInfoConsumer = new GcNotificationConsumer(publisher.consumer());
        publisher.setFlushDelegate(notificationInfoConsumer::flush);
        final Table notificationInfo = adapter.table();
        state.setField(name, notificationInfo);
        if (notificationInfoStatsEnabled()) {
            state.setField(String.format("%s_stats", name), GcNotificationConsumer.stats(notificationInfo));
        }
        final int ringSize = notificationInfoRingSize();
        if (ringSize > 0) {
            state.setField(String.format("%s_ring", name), RingTableTools.of(notificationInfo, ringSize));
        }
    }

    private void setPools(ApplicationState state) {
        final String name = "pools";
        final SetableStreamPublisher publisher = new SetableStreamPublisher();
        // noinspection resource
        final StreamToTableAdapter adapter = new StreamToTableAdapter(GcPoolsConsumer.definition(), publisher,
                UpdateGraphProcessor.DEFAULT, name);
        poolsConsumer = new GcPoolsConsumer(publisher.consumer());
        publisher.setFlushDelegate(poolsConsumer::flush);

        final Table pools = adapter.table();
        state.setField(name, pools);
        if (poolStatsEnabled()) {
            state.setField(String.format("%s_stats", name), GcPoolsConsumer.stats(pools));
        }
    }

    private void install() {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).addNotificationListener(this, null, null);
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
}
