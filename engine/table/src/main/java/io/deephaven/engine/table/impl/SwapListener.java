/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.reference.SwappableDelegatingReference;
import io.deephaven.base.reference.WeakSimpleReference;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.updategraph.*;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * Watch for ticks and when initialization is complete forward to the eventual listener.
 * <p>
 * The SwapListener is attached to a table so that we can listen for updates during the UGP cycle; and if any updates
 * occur, we'll be able to notice them and retry initialization. If no ticks were received before the result is ready,
 * then we should forward all calls to our eventual listener.
 * <p>
 * Callers should use our start and end functions. The start function is called at the beginning of a data snapshot; and
 * allows us to setup our state variables. At the end of the snapshot attempt, end() is called; and if there were no
 * clock changes, we were not gotNotification, and no notifications were enqueued; then we have a successful snapshot
 * and can return true. We then set the {@code eventualListener}, so that all future calls are forwarded to the
 * listener, and replace our
 */
public class SwapListener extends LivenessArtifact implements TableUpdateListener {

    static final boolean DEBUG =
            Configuration.getInstance().getBooleanWithDefault("SwapListener.debug", false);
    static final boolean DEBUG_NOTIFICATIONS =
            Configuration.getInstance().getBooleanWithDefault("SwapListener.debugNotifications", false);

    private static final Logger log = LoggerFactory.getLogger(SwapListener.class);

    /**
     * The listener that will be called if this operation is successful. If we have a successful snapshot, then success
     * is set to true.
     */
    private TableUpdateListener eventualListener;
    private NotificationStepReceiver eventualResult;
    boolean success = false;

    /**
     * The last clock cycle which the source table produced a notification.
     */
    long lastNotificationStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;

    /**
     * The sourceTable, used to get the lastNotificationTime.
     */
    final BaseTable sourceTable;

    /**
     * {@link WeakSimpleReference} to {@code this}, for capturing notifications from {@code sourceTable} before
     * successful {@link #end(long)}.
     */
    private final SimpleReference<TableUpdateListener> initialDelegate = new WeakSimpleReference<>(this);
    /**
     * {@link SwappableDelegatingReference}, to be swapped to a reference to the {@code eventualListener} upon
     * successful {@link #end(long)}.
     */
    private final SwappableDelegatingReference<TableUpdateListener> referenceForSource =
            new SwappableDelegatingReference<>(initialDelegate);

    public SwapListener(final BaseTable sourceTable) {
        this.sourceTable = sourceTable;
    }

    public ConstructSnapshot.SnapshotControl makeSnapshotControl() {
        // noinspection AutoBoxing
        return ConstructSnapshot.makeSnapshotControl(
                this::start,
                (final long currentClockValue, final boolean usingPreviousValues) -> isInInitialNotificationWindow(),
                (final long afterClockValue, final boolean usedPreviousValues) -> end(afterClockValue));
    }

    /**
     * Starts a snapshot.
     *
     * @param clockCycle the clockCycle we are starting a snapshot on
     * @return true if we should use previous values, false if we should use current values.
     */
    protected synchronized boolean start(final long clockCycle) {
        lastNotificationStep = sourceTable.getLastNotificationStep();
        success = false;
        final long currentStep = LogicalClock.getStep(clockCycle);
        final boolean updatedOnThisCycle = currentStep == lastNotificationStep;
        final boolean updating = LogicalClock.getState(clockCycle) == LogicalClock.State.Updating;
        if (DEBUG) {
            log.info().append("SwapListener {source=").append(System.identityHashCode(sourceTable))
                    .append(", swap=").append(System.identityHashCode(this))
                    .append("} Start: currentStep=").append(currentStep)
                    .append(", last=").append(lastNotificationStep)
                    .append(", updating=").append(updating)
                    .append(", updatedOnThisCycle=").append(updatedOnThisCycle)
                    .endl();
        }
        return updating && !updatedOnThisCycle;
    }

    /**
     * Ends a snapshot. Overriding methods must call {@code super} in order to ensure that the source's reference to the
     * child listener is properly swapped to the eventual listener.
     *
     * @param clockCycle The {@link LogicalClock logical clock} cycle we are ending a snapshot on
     * @return true if the snapshot was successful, false if we should try again.
     * @throws IllegalStateException If the snapshot was successful (consistent), but the snapshot function failed to
     *         set the eventual listener or eventual result
     */
    @OverridingMethodsMustInvokeSuper
    protected synchronized boolean end(@SuppressWarnings("unused") final long clockCycle) {
        if (isInInitialNotificationWindow()) {
            if (eventualListener == null) {
                throw new IllegalStateException("Listener has not been set on end!");
            }
            if (eventualResult == null) {
                throw new IllegalStateException("Result has not been set on end!");
            }
            success = true;
        } else {
            success = false;
        }

        if (DEBUG) {
            log.info().append("SwapListener {source=").append(System.identityHashCode(sourceTable))
                    .append(" swap=").append(System.identityHashCode(this))
                    .append("} End: success=").append(success)
                    .append(", last=").append(lastNotificationStep)
                    .endl();
        }

        if (success) {
            eventualResult.setLastNotificationStep(lastNotificationStep);
            referenceForSource.swapDelegate(initialDelegate, eventualListener instanceof LegacyListenerAdapter
                    ? (LegacyListenerAdapter) eventualListener
                    : new WeakSimpleReference<>(eventualListener));
        }

        return success;
    }

    /**
     * Get a {@link SimpleReference} to be used in the source table's list of child listener references.
     *
     * @return A (swappable) {@link SimpleReference} to {@code this}
     */
    public SimpleReference<TableUpdateListener> getReferenceForSource() {
        return referenceForSource;
    }

    @Override
    public synchronized void onFailure(
            final Throwable originalException, final Entry sourceEntry) {
        // not a direct listener
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized NotificationQueue.ErrorNotification getErrorNotification(
            final Throwable originalException, final Entry sourceEntry) {
        if (!success || isInInitialNotificationWindow()) {
            return new EmptyErrorNotification();
        }
        return eventualListener.getErrorNotification(originalException, sourceEntry);
    }

    @Override
    public synchronized void onUpdate(final TableUpdate upstream) {
        // not a direct listener
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized NotificationQueue.Notification getNotification(final TableUpdate upstream) {
        if (!success || isInInitialNotificationWindow()) {
            return new EmptyNotification();
        }

        final NotificationQueue.Notification notification = eventualListener.getNotification(upstream);
        if (!DEBUG_NOTIFICATIONS) {
            return notification;
        }

        return new AbstractNotification(notification.isTerminal()) {

            @Override
            public boolean canExecute(final long step) {
                return notification.canExecute(step);
            }

            @Override
            public LogOutput append(final LogOutput logOutput) {
                return logOutput.append("Wrapped(SwapListener {source=").append(System.identityHashCode(sourceTable))
                        .append(" swap=").append(System.identityHashCode(SwapListener.this))
                        .append("}, notification=").append(notification).append(")");
            }

            @Override
            public void run() {
                log.info().append("SwapListener {source=").append(System.identityHashCode(sourceTable))
                        .append(" swap=").append(System.identityHashCode(SwapListener.this))
                        .append(", clock=").append(LogicalClock.DEFAULT.currentStep())
                        .append("} Firing notification")
                        .endl();
                notification.runInContext();
                log.info().append("SwapListener {source=").append(System.identityHashCode(sourceTable))
                        .append(" swap=").append(System.identityHashCode(SwapListener.this))
                        .append("} Complete notification")
                        .endl();
            }

            @Override
            public ExecutionContext getExecutionContext() {
                return null;
            }
        };
    }

    boolean isInInitialNotificationWindow() {
        final long newNotificationStep = sourceTable.getLastNotificationStep();
        return lastNotificationStep == newNotificationStep;
    }

    /**
     * Set the listener that will eventually become the listener, if we have a successful swap.
     *
     * @param listener The listener that we will eventually forward all updates to
     * @param resultTable The table that will result from this operation
     */
    public synchronized void setListenerAndResult(
            @NotNull final TableUpdateListener listener,
            @NotNull final NotificationStepReceiver resultTable) {
        eventualListener = listener;
        eventualResult = resultTable;
        if (DEBUG) {
            log.info().append("SwapListener {source=").append(System.identityHashCode(sourceTable))
                    .append(", swap=").append(System.identityHashCode(this))
                    .append(", result=").append(System.identityHashCode(resultTable))
                    .append('}')
                    .endl();
        }
    }

    /**
     * Invoke {@link Table#addUpdateListener(TableUpdateListener) addUpdateListener} on {@code this}.
     */
    public void subscribeForUpdates() {
        sourceTable.addUpdateListener(this);
    }

    @Override
    public void destroy() {
        super.destroy();
        sourceTable.removeUpdateListener(this);
    }
}
