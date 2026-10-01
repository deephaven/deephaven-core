//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.*;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * A simple implementation of {@link ConstructSnapshot.SnapshotControl} that uses the last notification step of the
 * source table to determine whether to use previous values during initialization and to evaluate success.
 */
public class OperationSnapshotControl implements ConstructSnapshot.SnapshotControl {

    static final boolean DEBUG =
            Configuration.getInstance().getBooleanWithDefault("OperationSnapshotControl.debug", false);

    private static final Logger log = LoggerFactory.getLogger(OperationSnapshotControl.class);

    private TableUpdateListener eventualListener;
    private NotificationStepReceiver eventualResult;

    /**
     * The last clock cycle which the source table produced a notification.
     */
    long lastNotificationStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;

    /**
     * The sourceTable, used to get the lastNotificationTime.
     */
    final BaseTable<?> sourceTable;

    public OperationSnapshotControl(final BaseTable<?> sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return sourceTable.getUpdateGraph();
    }

    /**
     * Starts a snapshot. Overriding methods must call {@link #clearListenerAndResult()} in order to discard any
     * listener and result left behind by a previous attempt.
     *
     * @param beforeClockValue the logical clock value we are starting a snapshot on
     * @return true if we should use previous values, false if we should use current values.
     */
    @Override
    public synchronized Boolean usePreviousValues(final long beforeClockValue) {
        clearListenerAndResult();
        lastNotificationStep = sourceTable.getLastNotificationStep();

        final long beforeStep = LogicalClock.getStep(beforeClockValue);
        final LogicalClock.State beforeState = LogicalClock.getState(beforeClockValue);

        final boolean idle = beforeState == LogicalClock.State.Idle;
        final boolean updatedOnThisStep = beforeStep == lastNotificationStep;
        final boolean satisfied;
        try {
            satisfied = idle || updatedOnThisStep || sourceTable.satisfied(beforeStep);
        } catch (ClockInconsistencyException e) {
            return null;
        }
        final boolean usePrev = !satisfied;

        if (DEBUG) {
            log.info().append("OperationSnapshotControl {source=").append(System.identityHashCode(sourceTable))
                    .append(", control=").append(System.identityHashCode(this))
                    .append("} usePreviousValues: beforeStep=").append(beforeStep)
                    .append(", beforeState=").append(beforeState.name())
                    .append(", lastNotificationStep=").append(lastNotificationStep)
                    .append(", satisfied=").append(satisfied)
                    .append(", usePrev=").append(usePrev)
                    .endl();
        }
        return usePrev;
    }

    @Override
    public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
        return isInInitialNotificationWindow();
    }

    /**
     * Ends a snapshot. Overriding methods must call {@code super} in order to ensure that the result's last
     * notification step is properly set.
     *
     * @param afterClockValue The {@link LogicalClock logical clock} cycle we are ending a snapshot on
     * @param usedPreviousValues Whether we used previous values during the snapshot
     * @return true if the snapshot was successful, false if we should try again.
     * @throws IllegalStateException If the snapshot was successful (consistent), but the snapshot function failed to
     *         set the eventual listener or eventual result
     */
    @Override
    @OverridingMethodsMustInvokeSuper
    public synchronized boolean snapshotCompletedConsistently(
            final long afterClockValue,
            final boolean usedPreviousValues) {
        final boolean snapshotConsistent;
        if (isInInitialNotificationWindow()) {
            if (eventualResult == null) {
                throw new IllegalStateException("Result has not been set on end!");
            }
            snapshotConsistent = true;
        } else {
            snapshotConsistent = false;
        }

        if (DEBUG) {
            log.info().append("OperationSnapshotControl {source=").append(System.identityHashCode(sourceTable))
                    .append(" control=").append(System.identityHashCode(this))
                    .append("} snapshotCompletedConsistently: afterClockValue=").append(afterClockValue)
                    .append(", usedPreviousValues=").append(usedPreviousValues)
                    .append(", snapshotConsistent=").append(snapshotConsistent)
                    .append(", lastNotificationStep=").append(lastNotificationStep)
                    .endl();
        }

        if (!snapshotConsistent) {
            return false;
        }

        // Be sure to record the result's initial last notification step before subscribing.
        eventualResult.setLastNotificationStep(lastNotificationStep);
        if (!maybeSubscribeDependencies()) {
            // There were dependencies that could not be subscribed consistently. Fail the snapshot.
            return false;
        }
        final boolean sourceSubscribed;
        try {
            sourceSubscribed = eventualListener == null || subscribeForUpdates(eventualListener);
        } catch (RuntimeException e) {
            maybeUnsubscribeDependencies();
            throw e;
        }
        if (!sourceSubscribed) {
            // The source table could not be subscribed consistently. Unwind the dependency subscriptions and fail the
            // snapshot.
            maybeUnsubscribeDependencies();
            return false;
        }
        return true;
    }

    /**
     * Subscribe to dependencies this operation needs (other than {@link #sourceTable}). A failure to subscribe any
     * dependency will reject the snapshot attempt.
     *
     * @return Whether every such dependency was subscribed
     */
    boolean maybeSubscribeDependencies() {
        return true;
    }

    /**
     * Undo {@link #maybeSubscribeDependencies()} when a later step rejects the attempt.
     */
    void maybeUnsubscribeDependencies() {}

    /**
     * @return Whether we are in the initial notification window and can continue with the snapshot
     */
    protected boolean isInInitialNotificationWindow() {
        final long newNotificationStep = sourceTable.getLastNotificationStep();
        return lastNotificationStep == newNotificationStep;
    }

    /**
     * Subscribe for updates from the source table.
     *
     * @param listener The listener to subscribe
     * @return Whether the subscription was successful
     */
    boolean subscribeForUpdates(@NotNull final TableUpdateListener listener) {
        return sourceTable.addUpdateListener(listener, lastNotificationStep);
    }

    /**
     * Discard the listener and result recorded by a previous attempt. Every attempt runs the snapshot function again
     * and must set them afresh, so an attempt whose function fails without setting them must not commit an earlier
     * attempt's already-discarded result.
     */
    synchronized void clearListenerAndResult() {
        eventualListener = null;
        eventualResult = null;
    }

    /**
     * Set the listener that will eventually become the listener, if we have a successful snapshot.
     *
     * @param listener The listener that we will eventually forward all updates to
     * @param resultTable The table that will result from this operation
     */
    public synchronized void setListenerAndResult(
            final TableUpdateListener listener,
            @NotNull final NotificationStepReceiver resultTable) {
        eventualListener = listener;
        eventualResult = resultTable;
        if (DEBUG) {
            log.info().append("OperationSnapshotControl {source=").append(System.identityHashCode(sourceTable))
                    .append(", control=").append(System.identityHashCode(this))
                    .append(", result=").append(System.identityHashCode(resultTable))
                    .append('}')
                    .endl();
        }
    }
}
