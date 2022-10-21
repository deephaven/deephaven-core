package io.deephaven.app;

import com.google.auto.service.AutoService;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.stream.StreamToTableAdapter;
import io.deephaven.util.SafeCloseable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@AutoService(ApplicationState.Factory.class)
public final class ThreadApplication implements ApplicationState.Factory {

    private static final String APP_ID = "io.deephaven.app.ThreadApplication";
    private static final String APP_NAME = "Thread Application";
    private static final String ENABLED = "io.deephaven.app.ThreadApplication.enabled";
    private static final String THREADS_PERIOD = "io.deephaven.app.ThreadApplication.period";
    private static final String THREADS_STATS_ENABLED = "io.deephaven.app.ThreadApplication.threads_stats.enabled";
    private static final String THREADS_RING_SIZE = "io.deephaven.app.ThreadApplication.threads_ring.size";
    public static final String THREADS = "threads";
    public static final String THREADS_STATS = "threads_stats";
    public static final String THREADS_RING = "threads_ring";

    /**
     * Looks up the system property {@value ENABLED}, defaults to {@code false}.
     *
     * @return if the thread application is enabled
     */
    public static boolean enabled() {
        // Note: this is off-by-default until the Web UI has been updated to better handle on-by-default applications.
        return "true".equalsIgnoreCase(System.getProperty(ENABLED));
    }

    /**
     * Looks up the system property {@value THREADS_STATS_ENABLED}, defaults to {@code true}.
     *
     * @return if {@value THREADS_STATS} table is enabled
     */
    public static boolean threadsStatsEnabled() {
        return "true".equalsIgnoreCase(System.getProperty(THREADS_STATS_ENABLED, "true"));
    }

    /**
     * Looks up the system property {@value THREADS_RING_SIZE}, defaults to {@code 8192}. The {@value THREADS_RING} table is disabled when {@code 0} or less.
     *
     * @return the {@value THREADS_RING} table size
     */
    public static int threadsRingSize() {
        return Integer.getInteger(THREADS_RING_SIZE, 8192);
    }

    /**
     * Looks up the system property {@value THREADS_PERIOD}, defaults to {@code PT1s}, parsed via {@link Duration#parse(CharSequence)}.
     *
     * @return the period to poll thread information
     */
    public static Duration period() {
        return Duration.parse(System.getProperty(THREADS_PERIOD, "PT1s"));
    }

    private ThreadPublisher threadPublisher;

    @SuppressWarnings("FieldCanBeLocal")
    private LivenessScope scope;

    @Override
    public ApplicationState create(Listener listener) {
        final ApplicationState state = new ApplicationState(listener, APP_ID, APP_NAME);
        if (!enabled()) {
            return state;
        }
        scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            threadPublisher = new ThreadPublisher();
            final StreamToTableAdapter adapter = new StreamToTableAdapter(ThreadPublisher.definition(),
                    threadPublisher, UpdateGraphProcessor.DEFAULT, THREADS);

            final Table threadsTable = adapter.table();
            state.setField(THREADS, threadsTable);

            if (threadsStatsEnabled()) {
                final Table threadsStats = threadsTable.aggBy(List.of(
                                Aggregation.AggLast("Timestamp", "Name", "State", "IsInNative", "IsDaemon", "Priority"),
                                Aggregation.AggSum("BlockedCount", "BlockedDuration", "WaitedCount", "WaitedDuration", "UserTime", "SystemTime", "AllocatedBytes")),
                        "Id");
                state.setField(THREADS_STATS, threadsStats);
            }

            final int ringSize = threadsRingSize();
            if (ringSize > 0) {
                state.setField(THREADS_RING, RingTableTools.of(threadsTable, ringSize));
            }
        }

        // pass in scheduler
        // TODO(deephaven-core#3037): Provide io.deephaven.appmode.ApplicationState.Factory via a Set
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(threadPublisher::measure, 0, period().toNanos(), TimeUnit.NANOSECONDS);

        return state;
    }
}
