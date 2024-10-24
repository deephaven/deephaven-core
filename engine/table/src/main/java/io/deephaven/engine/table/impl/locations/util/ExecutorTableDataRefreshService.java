//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.util.thread.NamingThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * {@link TableDataRefreshService} implementation that uses a {@link ScheduledThreadPoolExecutor}.
 */
public class ExecutorTableDataRefreshService implements TableDataRefreshService {

    private static final String NAME_PREFIX = "TableDataRefreshService-";

    private final String name;
    private final long tableLocationProviderDefaultRefreshIntervalMillis;
    private final long tableLocationDefaultRefreshIntervalMillis;

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
        this.tableLocationProviderDefaultRefreshIntervalMillis =
                Require.gtZero(tableLocationProviderRefreshIntervalMillis,
                        "tableLocationProviderRefreshIntervalMillis");
        this.tableLocationDefaultRefreshIntervalMillis =
                Require.gtZero(tableLocationRefreshIntervalMillis, "tableLocationRefreshIntervalMillis");

        NamingThreadFactory threadFactory = new NamingThreadFactory(TableDataRefreshService.class, "refreshThread");
        scheduler =
                new ScheduledThreadPoolExecutor(threadPoolSize, threadFactory, new ThreadPoolExecutor.AbortPolicy());
        scheduler.setRemoveOnCancelPolicy(true);

        providerSubscriptions = Stats.makeItem(NAME_PREFIX + name, "providerSubscriptions", Counter.FACTORY).getValue();
        providerSubscriptionRefreshDurationNanos = Stats
                .makeItem(NAME_PREFIX + name, "providerSubscriptionRefreshDurationNanos", State.FACTORY).getValue();
        locationSubscriptions = Stats.makeItem(NAME_PREFIX + name, "locationSubscriptions", Counter.FACTORY).getValue();
        locationSubscriptionRefreshDurationNanos = Stats
                .makeItem(NAME_PREFIX + name, "locationSubscriptionRefreshDurationNanos", State.FACTORY).getValue();
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

        private ScheduledSubscriptionTask(
                @NotNull final TYPE subscriptionAggregator,
                final long refreshIntervalMillis) {
            this.subscriptionAggregator = subscriptionAggregator;
            future = scheduler.scheduleAtFixedRate(this::doRefresh, 0, refreshIntervalMillis, TimeUnit.MILLISECONDS);
        }

        private void doRefresh() {
            try {
                refresh();
            } catch (TableDataException e) {
                subscriptionAggregator.activationFailed(this, e);
            } catch (Throwable t) {
                subscriptionAggregator.activationFailed(this, new TableDataException("Unexpected error", t));
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

        private ScheduledTableLocationProviderRefresh(
                @NotNull final AbstractTableLocationProvider tableLocationProvider,
                final long refreshIntervalMillis) {
            super(tableLocationProvider, refreshIntervalMillis);
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

        private ScheduledTableLocationRefresh(
                @NotNull final AbstractTableLocation tableLocation,
                final long refreshIntervalMillis) {
            super(tableLocation, refreshIntervalMillis);
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
        return scheduleTableLocationProviderRefresh(tableLocationProvider,
                tableLocationProviderDefaultRefreshIntervalMillis);
    }

    @Override
    public CancellableSubscriptionToken scheduleTableLocationProviderRefresh(
            @NotNull final AbstractTableLocationProvider tableLocationProvider,
            final long refreshIntervalMillis) {
        return new ScheduledTableLocationProviderRefresh(tableLocationProvider, refreshIntervalMillis);
    }

    @Override
    public CancellableSubscriptionToken scheduleTableLocationRefresh(
            @NotNull final AbstractTableLocation tableLocation) {
        return scheduleTableLocationRefresh(tableLocation, tableLocationDefaultRefreshIntervalMillis);
    }

    @Override
    public CancellableSubscriptionToken scheduleTableLocationRefresh(
            @NotNull final AbstractTableLocation tableLocation,
            final long refreshIntervalMillis) {
        return new ScheduledTableLocationRefresh(tableLocation, refreshIntervalMillis);
    }
}
