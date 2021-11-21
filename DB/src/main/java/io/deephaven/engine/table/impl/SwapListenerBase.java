package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.updategraph.*;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.jetbrains.annotations.NotNull;

/**
 * Watch for ticks and when initialization is complete forward to the eventual listener.
 *
 * The SwapListenerBase is attached to a table so that we can listen for updates during the UGP cycle; and if any
 * updates occur, we'll be able to notice them and retry initialization. If no ticks were received before the result is
 * ready, then we should forward all calls to our eventual listener.
 *
 * Callers should use our start and end functions. The start function is called at the beginning of a data snapshot; and
 * allows us to setup our state variables. At the end of the snapshot attempt, end() is called; and if there were no
 * clock changes, we were not gotNotification, and no notifications were enqueued; then we have a successful snapshot
 * and can return true. We then set the currentListener, so that all future calls are forwarded to the listener.
 *
 * Use either {@link ShiftObliviousSwapListener} or {@link SwapListener} depending on which ShiftObliviousListener
 * interface you are using.
 */
public abstract class SwapListenerBase<T extends TableListener> extends LivenessArtifact implements TableListener {
    protected static final boolean DEBUG =
            Configuration.getInstance().getBooleanWithDefault("SwapListener.debug", false);
    static final boolean DEBUG_NOTIFICATIONS =
            Configuration.getInstance().getBooleanWithDefault("SwapListener.debugNotifications", false);

    private static final Logger log = LoggerFactory.getLogger(SwapListenerBase.class);

    /**
     * The listener that will be called if this operation is successful. If we have a successful snapshot, then success
     * is set to true.
     */
    T eventualListener;
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

    public SwapListenerBase(final BaseTable sourceTable) {
        this.sourceTable = sourceTable;
        manage(sourceTable);
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
            log.info().append("Swap ShiftObliviousListener source=")
                    .append(System.identityHashCode(sourceTable))
                    .append(" swap=")
                    .append(System.identityHashCode(this))
                    .append(" start: ")
                    .append(currentStep)
                    .append(" ")
                    .append(LogicalClock.getState(clockCycle).toString())
                    .append(", last=").append(lastNotificationStep)
                    .append(", updating=")
                    .append(updating)
                    .append(", updatedOnThisCycle=")
                    .append(updatedOnThisCycle).endl();
        }
        return updating && !updatedOnThisCycle;
    }

    /**
     * Ends a snapshot.
     *
     * @param clockCycle The {@link LogicalClock logical clock} cycle we are ending a snapshot on
     * @return true if the snapshot was successful, false if we should try again.
     * @throws IllegalStateException If the snapshot was successful (consistent), but the snapshot function failed to
     *         set the eventual listener or eventual result
     */
    protected synchronized boolean end(@SuppressWarnings("unused") final long clockCycle) {
        if (isInInitialNotificationWindow()) {
            if (eventualListener == null) {
                throw new IllegalStateException("ShiftObliviousListener has not been set on end!");
            }
            if (eventualResult == null) {
                throw new IllegalStateException("Result has not been set on end!");
            }
            success = true;
        } else {
            success = false;
        }

        if (DEBUG) {
            log.info().append("Swap ShiftObliviousListener ")
                    .append(System.identityHashCode(sourceTable))
                    .append(" swap=")
                    .append(System.identityHashCode(this))
                    .append(" End: success=")
                    .append(success)
                    .append(", last=")
                    .append(lastNotificationStep).endl();
        }

        if (success) {
            eventualResult.setLastNotificationStep(lastNotificationStep);
        }

        return success;
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
        if (success && !isInInitialNotificationWindow()) {
            return eventualListener.getErrorNotification(originalException, sourceEntry);
        } else {
            return new EmptyErrorNotification();
        }
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
    public synchronized void setListenerAndResult(@NotNull final T listener,
            @NotNull final NotificationStepReceiver resultTable) {
        eventualListener = listener;
        eventualResult = resultTable;
        if (DEBUG) {
            log.info().append("SwapListener source=")
                    .append(System.identityHashCode(sourceTable))
                    .append(", swap=")
                    .append(System.identityHashCode(this)).append(", result=")
                    .append(System.identityHashCode(resultTable)).endl();
        }
    }

    /**
     * Invoke {@link QueryTable#listenForUpdates(ShiftObliviousListener)} for the appropriate subclass of
     * {@link SwapListenerBase}.
     */
    public abstract void subscribeForUpdates();

    interface NotificationFactory {
        NotificationQueue.Notification newNotification();
    }

    NotificationQueue.Notification doGetNotification(final NotificationFactory factory) {
        if (!success || isInInitialNotificationWindow()) {
            return new EmptyNotification();
        }

        final NotificationQueue.Notification notification = factory.newNotification();
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
                return logOutput.append("Wrapped(SwapListener=")
                        .append(System.identityHashCode(sourceTable))
                        .append(" swap=")
                        .append(System.identityHashCode(SwapListenerBase.this))
                        .append("){")
                        .append(notification)
                        .append("}");
            }

            @Override
            public void run() {
                log.info().append("SwapListener: Firing notification ")
                        .append(System.identityHashCode(sourceTable))
                        .append(" swap=")
                        .append(System.identityHashCode(SwapListenerBase.this))
                        .append(", clock=")
                        .append(LogicalClock.DEFAULT.currentStep()).endl();
                notification.run();
                log.info().append("SwapListener: Complete notification ")
                        .append(System.identityHashCode(sourceTable))
                        .append(" swap=")
                        .append(System.identityHashCode(SwapListenerBase.this)).endl();
            }
        };
    }
}
