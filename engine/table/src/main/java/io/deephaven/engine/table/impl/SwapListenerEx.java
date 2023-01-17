package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.WaitNotification;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * Variant of {@link SwapListener} that considers an "extra" {@link NotificationStepSource} in addition to the source
 * {@link BaseTable} when determining whether to use previous values during initialization or evaluating success. This
 * is useful anytime an operation needs to listen to and snapshot one data source while also snapshotting another.
 */
public final class SwapListenerEx extends SwapListener {

    private static final Logger log = LoggerFactory.getLogger(SwapListenerEx.class);

    private final NotificationStepSource extra;

    private long extraLastNotificationStep;

    public SwapListenerEx(@NotNull final BaseTable sourceTable, @NotNull final NotificationStepSource extra) {
        super(sourceTable);
        this.extra = extra;
    }

    @Override
    public ConstructSnapshot.SnapshotControl makeSnapshotControl() {
        return ConstructSnapshot.makeSnapshotControl(
                this::startWithExtra,
                (final long currentClockValue, final boolean usingPreviousValues) -> isInInitialNotificationWindow()
                        && extra.getLastNotificationStep() == extraLastNotificationStep,
                (final long afterClockValue, final boolean usedPreviousValues) -> end(afterClockValue));
    }

    @SuppressWarnings("AutoBoxing")
    public synchronized Boolean startWithExtra(final long beforeClockValue) {
        lastNotificationStep = sourceTable.getLastNotificationStep();
        extraLastNotificationStep = extra.getLastNotificationStep();
        success = false;

        final long beforeStep = LogicalClock.getStep(beforeClockValue);
        final LogicalClock.State beforeState = LogicalClock.getState(beforeClockValue);

        final Boolean result;
        if (beforeState == LogicalClock.State.Idle) {
            result = false;
        } else {
            final boolean sourceUpdatedOnThisCycle = lastNotificationStep == beforeStep;
            final boolean extraUpdatedOnThisCycle = extraLastNotificationStep == beforeStep;

            if (sourceUpdatedOnThisCycle) {
                if (extraUpdatedOnThisCycle || extra.satisfied(beforeStep)) {
                    result = false;
                } else {
                    WaitNotification.waitForSatisfaction(beforeStep, extra);
                    extraLastNotificationStep = extra.getLastNotificationStep();
                    result = LogicalClock.DEFAULT.currentStep() == beforeStep ? false : null;
                }
            } else if (extraUpdatedOnThisCycle) {
                if (sourceTable.satisfied(beforeStep)) {
                    result = false;
                } else {
                    WaitNotification.waitForSatisfaction(beforeStep, sourceTable);
                    lastNotificationStep = sourceTable.getLastNotificationStep();
                    result = LogicalClock.DEFAULT.currentStep() == beforeStep ? false : null;
                }
            } else {
                result = true;
            }
        }
        if (DEBUG) {
            log.info().append("SwapListenerEx start() source=")
                    .append(System.identityHashCode(sourceTable))
                    .append(". swap=")
                    .append(System.identityHashCode(this))
                    .append(", start={").append(beforeStep).append(",").append(beforeState.toString())
                    .append("}, last=").append(lastNotificationStep)
                    .append(", extraLast=").append(extraLastNotificationStep)
                    .append(", result=").append(result)
                    .endl();
        }
        return result;
    }

    @Override
    public boolean start(final long beforeClockValue) {
        throw new UnsupportedOperationException("Use startWithExtra");
    }

    @Override
    public synchronized boolean end(final long afterClockValue) {
        if (DEBUG) {
            log.info().append("SwapListenerEx end() swap=").append(System.identityHashCode(this))
                    .append(", end={").append(LogicalClock.getStep(afterClockValue)).append(",")
                    .append(LogicalClock.getState(afterClockValue).toString())
                    .append("}, last=").append(sourceTable.getLastNotificationStep())
                    .append(", extraLast=").append(extra.getLastNotificationStep())
                    .endl();
        }
        return extra.getLastNotificationStep() == extraLastNotificationStep && super.end(afterClockValue);
    }

    @Override
    public synchronized void setListenerAndResult(@NotNull final TableUpdateListener listener,
            @NotNull final NotificationStepReceiver resultTable) {
        super.setListenerAndResult(listener, resultTable);
        if (DEBUG) {
            log.info().append("SwapListenerEx swap=")
                    .append(System.identityHashCode(SwapListenerEx.this))
                    .append(", result=").append(System.identityHashCode(resultTable)).endl();
        }
    }
}
