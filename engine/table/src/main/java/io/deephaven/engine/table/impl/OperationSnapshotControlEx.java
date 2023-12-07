package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.updategraph.ClockInconsistencyException;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.WaitNotification;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Variant of {@link OperationSnapshotControl} that considers "extra" {@link NotificationStepSource sources} in addition
 * to the source {@link BaseTable} when determining whether to use previous values during initialization or evaluating
 * success. This is useful anytime an operation needs to listen to and snapshot one data source while also snapshotting
 * others.
 */
public final class OperationSnapshotControlEx extends OperationSnapshotControl {

    private static final Logger log = LoggerFactory.getLogger(OperationSnapshotControlEx.class);

    private final NotificationStepSource[] extras;

    private long extraLastNotificationStep[];

    public OperationSnapshotControlEx(
            @NotNull final BaseTable<?> sourceTable,
            @NotNull final NotificationStepSource... extras) {
        super(sourceTable);
        this.extras = extras;
        extraLastNotificationStep = new long[extras.length];
    }

    @Override
    @SuppressWarnings("AutoBoxing")
    public synchronized Boolean usePreviousValues(final long beforeClockValue) {
        lastNotificationStep = sourceTable.getLastNotificationStep();

        final long beforeStep = LogicalClock.getStep(beforeClockValue);
        final LogicalClock.State beforeState = LogicalClock.getState(beforeClockValue);

        final boolean idle = beforeState == LogicalClock.State.Idle;
        final boolean sourceUpdatedOnThisStep = lastNotificationStep == beforeStep;
        final boolean sourceSatisfied;

        final boolean[] extraUpdatedOnThisStep = new boolean[extras.length];
        final boolean[] extraSatisfied = new boolean[extras.length];

        boolean extraSatisfiedAll = true;

        try {
            sourceSatisfied = idle || sourceUpdatedOnThisStep || sourceTable.satisfied(beforeStep);
            for (int ii = 0; ii < extras.length; ++ii) {
                extraLastNotificationStep[ii] = extras[ii].getLastNotificationStep();
                extraUpdatedOnThisStep[ii] = extraLastNotificationStep[ii] == beforeStep;
                extraSatisfied[ii] = idle || extraUpdatedOnThisStep[ii] || extras[ii].satisfied(beforeStep);

                // Clear the satisfied flag if this extra is not satisfied.
                extraSatisfiedAll &= extraSatisfied[ii];
            }
        } catch (ClockInconsistencyException e) {
            return null;
        }

        final Boolean usePrev;
        if (sourceSatisfied == extraSatisfiedAll) {
            usePrev = !sourceSatisfied;
        } else if (sourceSatisfied) {
            WaitNotification.waitForSatisfaction(beforeStep, extras);
            for (int ii = 0; ii < extras.length; ++ii) {
                extraLastNotificationStep[ii] = extras[ii].getLastNotificationStep();
            }
            final long postWaitStep = ExecutionContext.getContext().getUpdateGraph().clock().currentStep();
            usePrev = postWaitStep == beforeStep ? false : null;
        } else {
            WaitNotification.waitForSatisfaction(beforeStep, sourceTable);
            lastNotificationStep = sourceTable.getLastNotificationStep();
            final long postWaitStep = ExecutionContext.getContext().getUpdateGraph().clock().currentStep();
            usePrev = postWaitStep == beforeStep ? false : null;
        }

        if (DEBUG) {
            final String extraHashCodes = Arrays.stream(extras).map(e -> Integer.toString(System.identityHashCode(e)))
                    .collect(Collectors.joining(","));

            log.info().append("OperationSnapshotControlEx {source=").append(System.identityHashCode(sourceTable))
                    .append(", extra=").append(extraHashCodes)
                    .append(", control=").append(System.identityHashCode(this))
                    .append("} usePreviousValues: beforeStep=").append(beforeStep)
                    .append(", beforeState=").append(beforeState.name())
                    .append(", sourceLastNotificationStep=").append(lastNotificationStep)
                    .append(", sourceSatisfied=").append(sourceSatisfied)
                    .append(", extraLastNotificationStep=").append(Arrays.toString(extraLastNotificationStep))
                    .append(", extraSatisfied=").append(Arrays.toString(extraSatisfied))
                    .append(", usePrev=").append(usePrev)
                    .endl();
        }
        return usePrev;
    }

    @Override
    public synchronized boolean snapshotCompletedConsistently(long afterClockValue, boolean usedPreviousValues) {
        if (DEBUG) {
            final long[] extraLastNotificationSteps = Arrays.stream(extras)
                    .mapToLong(NotificationStepSource::getLastNotificationStep)
                    .toArray();
            log.info().append("OperationSnapshotControlEx snapshotCompletedConsistently: control=")
                    .append(System.identityHashCode(this))
                    .append(", end={").append(LogicalClock.getStep(afterClockValue)).append(",")
                    .append(LogicalClock.getState(afterClockValue).toString())
                    .append("}, usedPreviousValues=").append(usedPreviousValues)
                    .append(", last=").append(sourceTable.getLastNotificationStep())
                    .append(", extraLast=").append(Arrays.toString(extraLastNotificationSteps))
                    .endl();
        }
        boolean extrasConsistent = IntStream.range(0, extras.length)
                .allMatch(ii -> extras[ii].getLastNotificationStep() == extraLastNotificationStep[ii]);
        return extrasConsistent && super.snapshotCompletedConsistently(afterClockValue, usedPreviousValues);
    }

    @Override
    public synchronized void setListenerAndResult(@NotNull final TableUpdateListener listener,
            @NotNull final NotificationStepReceiver resultTable) {
        super.setListenerAndResult(listener, resultTable);
        if (DEBUG) {
            log.info().append("OperationSnapshotControlEx control=")
                    .append(System.identityHashCode(OperationSnapshotControlEx.this))
                    .append(", result=").append(System.identityHashCode(resultTable)).endl();
        }
    }

    @Override
    protected boolean isInInitialNotificationWindow() {
        boolean extrasConsistent = IntStream.range(0, extras.length)
                .allMatch(ii -> extras[ii].getLastNotificationStep() == extraLastNotificationStep[ii]);
        return extrasConsistent && super.isInInitialNotificationWindow();
    }
}
