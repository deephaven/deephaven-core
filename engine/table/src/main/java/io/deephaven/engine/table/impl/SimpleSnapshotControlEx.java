package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.updategraph.ClockInconsistencyException;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.WaitNotification;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * Variant of {@link SimpleSnapshotControl} that considers an "extra" {@link NotificationStepSource} in addition to the
 * source {@link BaseTable} when determining whether to use previous values during initialization or evaluating success.
 * This is useful anytime an operation needs to listen to and snapshot one data source while also snapshotting another.
 */
public final class SimpleSnapshotControlEx extends SimpleSnapshotControl {

    private static final Logger log = LoggerFactory.getLogger(SimpleSnapshotControlEx.class);

    private final NotificationStepSource extra;

    private long extraLastNotificationStep;

    public SimpleSnapshotControlEx(
            @NotNull final BaseTable<?> sourceTable,
            @NotNull final NotificationStepSource extra) {
        super(sourceTable);
        this.extra = extra;
    }

    @Override
    @SuppressWarnings("AutoBoxing")
    public synchronized Boolean usePreviousValues(final long beforeClockValue) {
        lastNotificationStep = sourceTable.getLastNotificationStep();
        extraLastNotificationStep = extra.getLastNotificationStep();

        final long beforeStep = LogicalClock.getStep(beforeClockValue);
        final LogicalClock.State beforeState = LogicalClock.getState(beforeClockValue);

        final boolean idle = beforeState == LogicalClock.State.Idle;
        final boolean sourceUpdatedOnThisStep = lastNotificationStep == beforeStep;
        final boolean sourceSatisfied;
        final boolean extraUpdatedOnThisStep = extraLastNotificationStep == beforeStep;
        final boolean extraSatisfied;

        try {
            sourceSatisfied = idle || sourceUpdatedOnThisStep || sourceTable.satisfied(beforeStep);
            extraSatisfied = idle || extraUpdatedOnThisStep || extra.satisfied(beforeStep);
        } catch (ClockInconsistencyException e) {
            return null;
        }

        final Boolean usePrev;
        if (sourceSatisfied == extraSatisfied) {
            usePrev = !sourceSatisfied;
        } else if (sourceSatisfied) {
            WaitNotification.waitForSatisfaction(beforeStep, extra);
            extraLastNotificationStep = extra.getLastNotificationStep();
            final long postWaitStep = ExecutionContext.getContext().getUpdateGraph().clock().currentStep();
            usePrev = postWaitStep == beforeStep ? false : null;
        } else {
            WaitNotification.waitForSatisfaction(beforeStep, sourceTable);
            lastNotificationStep = sourceTable.getLastNotificationStep();
            final long postWaitStep = ExecutionContext.getContext().getUpdateGraph().clock().currentStep();
            usePrev = postWaitStep == beforeStep ? false : null;
        }

        if (DEBUG) {
            log.info().append("SwapListenerEx {source=").append(System.identityHashCode(sourceTable))
                    .append(", extra=").append(System.identityHashCode(extra))
                    .append(", control=").append(System.identityHashCode(this))
                    .append("} Start: beforeStep=").append(beforeStep)
                    .append(", beforeState=").append(beforeState.name())
                    .append(", sourceLastNotificationStep=").append(lastNotificationStep)
                    .append(", sourceSatisfied=").append(sourceSatisfied)
                    .append(", extraLastNotificationStep=").append(extraLastNotificationStep)
                    .append(", extraSatisfied=").append(extraSatisfied)
                    .append(", usePrev=").append(usePrev)
                    .endl();
        }
        return usePrev;
    }

    @Override
    protected synchronized boolean end(final long afterClockValue) {
        if (DEBUG) {
            log.info().append("SimpleSnapshotControlEx end() control=").append(System.identityHashCode(this))
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
            log.info().append("SnapshotControlEx control=")
                    .append(System.identityHashCode(SimpleSnapshotControlEx.this))
                    .append(", result=").append(System.identityHashCode(resultTable)).endl();
        }
    }

    @Override
    protected boolean isInInitialNotificationWindow() {
        return extra.getLastNotificationStep() == extraLastNotificationStep && super.isInInitialNotificationWindow();
    }
}
