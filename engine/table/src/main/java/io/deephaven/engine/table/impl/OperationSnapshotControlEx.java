//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.updategraph.ClockInconsistencyException;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.WaitNotification;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.engine.updategraph.LogicalClock.NULL_CLOCK_VALUE;

/**
 * Variant of {@link OperationSnapshotControl} that considers "extra" {@link NotificationQueue.Dependency dependencies}
 * in addition to the source {@link BaseTable} when determining whether to use previous values during initialization or
 * evaluating success. This is useful anytime an operation needs to listen to and snapshot one data source while also
 * snapshotting others.
 */
public final class OperationSnapshotControlEx extends OperationSnapshotControl {

    private static final Logger log = LoggerFactory.getLogger(OperationSnapshotControlEx.class);

    private final NotificationQueue.Dependency[] extras;

    public OperationSnapshotControlEx(
            @NotNull final BaseTable<?> sourceTable,
            @NotNull final NotificationQueue.Dependency... extras) {
        super(sourceTable);
        this.extras = extras;
    }

    @Override
    @SuppressWarnings("AutoBoxing")
    public synchronized Boolean usePreviousValues(final long beforeClockValue) {
        lastNotificationStep = sourceTable.getLastNotificationStep();

        final long beforeStep = LogicalClock.getStep(beforeClockValue);
        final LogicalClock.State beforeState = LogicalClock.getState(beforeClockValue);

        if (beforeState == LogicalClock.State.Idle) {
            if (DEBUG) {
                log.info().append("OperationSnapshotControlEx {source=").append(System.identityHashCode(sourceTable))
                        .append(", extras=").append(Arrays.stream(extras)
                                .mapToInt(System::identityHashCode)
                                .mapToObj(Integer::toString)
                                .collect(Collectors.joining(", ", "[", "]")))
                        .append("} usePreviousValues: beforeStep=").append(beforeStep)
                        .append(", beforeState=").append(beforeState.name())
                        .append(", sourceLastNotificationStep=").append(lastNotificationStep)
                        .append(", usePrev=").append(false)
                        .endl();
            }
            return false;
        }

        final NotificationQueue.Dependency[] notYetSatisfied;
        try {
            notYetSatisfied = Stream.concat(Stream.of(sourceTable), Arrays.stream(extras))
                    .sequential()
                    .filter(dependency -> !satisfied(dependency, beforeStep))
                    .toArray(NotificationQueue.Dependency[]::new);
        } catch (ClockInconsistencyException e) {
            return null;
        }

        final long postWaitStep;
        final Boolean usePrev;
        if (notYetSatisfied.length == extras.length + 1) {
            // Nothing satisfied
            postWaitStep = NULL_CLOCK_VALUE;
            usePrev = true;
        } else if (notYetSatisfied.length > 0) {
            // Partially satisfied
            if (WaitNotification.waitForSatisfaction(beforeStep, notYetSatisfied)) {
                // Successful wait on beforeStep
                postWaitStep = beforeStep;
                usePrev = false;
            } else {
                // Updating phase finished before we could wait; use current if we're in the subsequent idle phase
                postWaitStep = getUpdateGraph().clock().currentStep();
                usePrev = postWaitStep == beforeStep ? false : null;
            }
        } else {
            // All satisfied
            postWaitStep = NULL_CLOCK_VALUE;
            usePrev = false;
        }

        if (DEBUG) {
            log.info().append("OperationSnapshotControlEx {source=").append(System.identityHashCode(sourceTable))
                    .append(", extras=").append(Arrays.stream(extras)
                            .mapToInt(System::identityHashCode)
                            .mapToObj(Integer::toString)
                            .collect(Collectors.joining(", ", "[", "]")))
                    .append(", control=").append(System.identityHashCode(this))
                    .append("} usePreviousValues: beforeStep=").append(beforeStep)
                    .append(", beforeState=").append(beforeState.name())
                    .append(", sourceLastNotificationStep=").append(lastNotificationStep)
                    .append(", notYetSatisfied=").append(Arrays.toString(notYetSatisfied))
                    .append(", postWaitStep=").append(postWaitStep)
                    .append(", usePrev=").append(usePrev)
                    .endl();
        }
        return usePrev;
    }

    private static boolean satisfied(@NotNull final NotificationQueue.Dependency dependency, final long step) {
        if (dependency instanceof NotificationStepSource
                && ((NotificationStepSource) dependency).getLastNotificationStep() == step) {
            return true;
        }
        return dependency.satisfied(step);
    }
}
