package io.deephaven.engine.table.impl.locations.util;

import io.deephaven.base.stats.Counter;
import io.deephaven.base.stats.State;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.Value;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.SubscriptionAggregator;
import io.deephaven.engine.table.impl.locations.TableDataException;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link TableDataRefreshService} implementation that uses a {@link ScheduledThreadPoolExecutor}.
 */
public class ExecutorTableDataRefreshService implements TableDataRefreshService {

    private static final String NAME_PREFIX = "TableDataRefreshService-";

    private final String name;
    private final long tableLocationProviderRefreshIntervalMillis;
    private final long tableLocationRefreshIntervalMillis;

    private final AtomicInteger threadCount = new AtomicInteger(0);

    private final ScheduledThreadPoolExecutor scheduler;

    private final Value providerSubscriptions;
    private final Value providerSubscriptionRefreshDurationNanos;
    private final Value locationSubscriptions;
    private final Value locationSubscriptionRefreshDurationNanos;

    public ExecutorTableDataRefreshService(@NotNull final String name,
            final long tableLocationProviderRefreshIntervalMillis,
            final long tableLocationRefreshIntervalMillis,
            final int threadPoolSize) {
        this.name = Require.neqNull(name, "name");
        this.tableLocationProviderRefreshIntervalMillis = Require.gtZero(tableLocationProviderRefreshIntervalMillis,
                "tableLocationProviderRefreshIntervalMillis");
        this.tableLocationRefreshIntervalMillis =
                Require.gtZero(tableLocationRefreshIntervalMillis, "tableLocationRefreshIntervalMillis");

        scheduler =
                new ScheduledThreadPoolExecutor(threadPoolSize, this::makeThread, new ThreadPoolExecutor.AbortPolicy());
        scheduler.setRemoveOnCancelPolicy(true);

        providerSubscriptions = Stats.makeItem(NAME_PREFIX + name, "providerSubscriptions", Counter.FACTORY).getValue();
        providerSubscriptionRefreshDurationNanos = Stats
                .makeItem(NAME_PREFIX + name, "providerSubscriptionRefreshDurationNanos", State.FACTORY).getValue();
        locationSubscriptions = Stats.makeItem(NAME_PREFIX + name, "locationSubscriptions", Counter.FACTORY).getValue();
        locationSubscriptionRefreshDurationNanos = Stats
                .makeItem(NAME_PREFIX + name, "locationSubscriptionRefreshDurationNanos", State.FACTORY).getValue();
    }

    private Thread makeThread(final Runnable runnable) {
        final Thread thread =
                new Thread(runnable, NAME_PREFIX + name + "-refreshThread-" + threadCount.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    }

    @Override
    public void submitOneTimeAsyncTask(@NotNull final Runnable task) {
        scheduler.submit(task);
    }

    private abstract class ScheduledSubscriptionTask<TYPE extends SubscriptionAggregator>
            implements CancellableSubscriptionToken {

        final TYPE subscriptionAggregator;

        private final Future<?> future;

        private volatile boolean firstInvocation = true;

        private ScheduledSubscriptionTask(@NotNull final TYPE subscriptionAggregator,
                final long refreshIntervalMillis) {
            this.subscriptionAggregator = subscriptionAggregator;
            future = scheduler.scheduleAtFixedRate(this::doRefresh, 0, refreshIntervalMillis, TimeUnit.MILLISECONDS);
        }

        private void doRefresh() {
            try {
                refresh();
            } catch (TableDataException e) {
                subscriptionAggregator.activationFailed(this, e);
            }
            if (firstInvocation) {
                firstInvocation = false;
                subscriptionAggregator.activationSuccessful(this);
            }
        }

        /**
         * Type-specific run processing.
         */
        protected abstract void refresh();

        @Override
        public void cancel() {
            future.cancel(false);
        }
    }

    private class ScheduledTableLocationProviderRefresh
            extends ScheduledSubscriptionTask<AbstractTableLocationProvider> {

        private ScheduledTableLocationProviderRefresh(@NotNull AbstractTableLocationProvider tableLocationProvider) {
            super(tableLocationProvider, tableLocationProviderRefreshIntervalMillis);
            providerSubscriptions.increment(1);
        }

        @Override
        protected void refresh() {
            final long startTimeNanos = System.nanoTime();
            subscriptionAggregator.refresh();
            providerSubscriptionRefreshDurationNanos.sample(System.nanoTime() - startTimeNanos);
        }

        @Override
        public void cancel() {
            super.cancel();
            providerSubscriptions.increment(-1);
        }
    }

    private class ScheduledTableLocationRefresh extends ScheduledSubscriptionTask<AbstractTableLocation> {

        private ScheduledTableLocationRefresh(@NotNull AbstractTableLocation tableLocation) {
            super(tableLocation, tableLocationRefreshIntervalMillis);
            locationSubscriptions.increment(1);
        }

        @Override
        protected void refresh() {
            final long startTimeNanos = System.nanoTime();
            subscriptionAggregator.refresh();
            locationSubscriptionRefreshDurationNanos.sample(System.nanoTime() - startTimeNanos);
        }

        @Override
        public void cancel() {
            super.cancel();
            locationSubscriptions.increment(-1);
        }
    }

    @Override
    public CancellableSubscriptionToken scheduleTableLocationProviderRefresh(
            @NotNull final AbstractTableLocationProvider tableLocationProvider) {
        return new ScheduledTableLocationProviderRefresh(tableLocationProvider);
    }

    @Override
    public CancellableSubscriptionToken scheduleTableLocationRefresh(
            @NotNull final AbstractTableLocation tableLocation) {
        return new ScheduledTableLocationRefresh(tableLocation);
    }
}
