package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.NotificationAdapter;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.io.log.LogEntry;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.function.ThrowingRunnable;
import io.deephaven.util.locks.AwareFunctionalLock;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An {@link UpdateGraph} implementation that wraps and delegates to an underlying source. It captures all update
 * sources so they can be refreshed manually
 */
public class CapturingUpdateGraph implements UpdateGraph {

    private final ControlledUpdateGraph delegate;

    private final ExecutionContext context;

    private final List<Runnable> sources = new ArrayList<>();

    public CapturingUpdateGraph(@NotNull final ControlledUpdateGraph delegate) {
        this.delegate = delegate;
        context = ExecutionContext.getContext().withUpdateGraph(this);
    }

    public ControlledUpdateGraph getDelegate() {
        return delegate;
    }

    public ExecutionContext getContext() {
        return context;
    }

    @Override
    public void addSource(@NotNull Runnable updateSource) {
        delegate.addSource(updateSource);
        sources.add(updateSource);
    }

    void refreshSources() {
        sources.forEach(Runnable::run);
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("CapturingUpdateGraph of ").append(delegate);
    }

    @Override
    public boolean satisfied(final long step) {
        return delegate.satisfied(step);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return this;
    }

    private Notification wrap(@NotNull final Notification notification) {
        return new NotificationAdapter(notification) {
            @Override
            public void run() {
                try (final SafeCloseable ignored = context.open()) {
                    super.run();
                }
            }
        };
    }

    @Override
    public void addNotification(@NotNull final Notification notification) {
        delegate.addNotification(wrap(notification));
    }

    @Override
    public void addNotifications(@NotNull final Collection<? extends Notification> notifications) {
        delegate.addNotifications(notifications.stream().map(this::wrap).collect(Collectors.toList()));
    }

    @Override
    public boolean maybeAddNotification(@NotNull final Notification notification, final long deliveryStep) {
        return delegate.maybeAddNotification(wrap(notification), deliveryStep);
    }

    @Override
    public String getName() {
        return "CapturingUpdateGraph";
    }

    @Override
    public AwareFunctionalLock sharedLock() {
        return delegate.sharedLock();
    }

    @Override
    public AwareFunctionalLock exclusiveLock() {
        return delegate.exclusiveLock();
    }

    @Override
    public LogicalClock clock() {
        return delegate.clock();
    }

    @Override
    public int parallelismFactor() {
        return delegate.parallelismFactor();
    }

    @Override
    public LogEntry logDependencies() {
        return delegate.logDependencies();
    }

    @Override
    public boolean currentThreadProcessesUpdates() {
        return delegate.currentThreadProcessesUpdates();
    }

    @Override
    public boolean serialTableOperationsSafe() {
        return delegate.serialTableOperationsSafe();
    }

    @Override
    public boolean setSerialTableOperationsSafe(final boolean newValue) {
        return delegate.serialTableOperationsSafe();
    }

    @Override
    public boolean supportsRefreshing() {
        return delegate.supportsRefreshing();
    }

    @Override
    public void requestRefresh() {
        delegate.requestRefresh();
    }

    @Override
    public void removeSource(@NotNull Runnable updateSource) {
        sources.remove(updateSource);
        delegate.removeSource(updateSource);
    }

    public <T extends Exception> void runWithinUnitTestCycle(@NotNull final ThrowingRunnable<T> runnable) throws T {
        delegate.runWithinUnitTestCycle(runnable);
    }

    public <T extends Exception> void runWithinUnitTestCycle(
            @NotNull final ThrowingRunnable<T> runnable,
            final boolean satisfied) throws T {
        delegate.runWithinUnitTestCycle(runnable, satisfied);
    }

    @Override
    public void stop() {
        delegate.stop();
    }
}
