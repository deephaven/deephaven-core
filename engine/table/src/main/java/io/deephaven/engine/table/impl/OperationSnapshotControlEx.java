//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
 * <p>
 * Extras are treated as <em>notification oblivious</em> by default: their notifications do not invalidate a snapshot,
 * because their previous values remain stable for the whole cycle and a snapshot using previous values reads them
 * consistently no matter when they tick. That is the correct treatment for the usual extras, which are tables or
 * table-backed structures such as data index tables.
 * <p>
 * An extra that also implements {@link NotificationAwareDependency} is the exception: it keeps no previous version of
 * the state it guards, so a snapshot that used previous values while that extra changed its state on the same step must
 * be retried. Such extras are detected automatically from those passed to the constructor, so callers need do nothing
 * beyond passing them as dependencies.
 */
public final class OperationSnapshotControlEx extends OperationSnapshotControl {

    private static final Logger log = LoggerFactory.getLogger(OperationSnapshotControlEx.class);

    private final NotificationQueue.Dependency[] extras;
    private final NotificationAwareDependency[] notificationAwareExtras;

    public OperationSnapshotControlEx(
            @NotNull final BaseTable<?> sourceTable,
            @NotNull final NotificationQueue.Dependency... extras) {
        super(sourceTable);
        this.extras = extras;
        this.notificationAwareExtras = Arrays.stream(extras)
                .filter(NotificationAwareDependency.class::isInstance)
                .map(NotificationAwareDependency.class::cast)
                .toArray(NotificationAwareDependency[]::new);
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

    @Override
    public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
        return notificationAwareExtrasConsistent(currentClockValue, usingPreviousValues)
                && super.snapshotConsistent(currentClockValue, usingPreviousValues);
    }

    @Override
    public synchronized boolean snapshotCompletedConsistently(
            final long afterClockValue,
            final boolean usedPreviousValues) {
        // Note that we must not delegate to super when we have already failed: on success it records the result's
        // last notification step and subscribes the eventual listener, which must not happen for a snapshot that is
        // about to be retried.
        if (!notificationAwareExtrasConsistent(afterClockValue, usedPreviousValues)) {
            if (DEBUG) {
                log.info().append("OperationSnapshotControlEx {source=")
                        .append(System.identityHashCode(sourceTable))
                        .append(", control=").append(System.identityHashCode(this))
                        .append("} snapshotCompletedConsistently: afterClockValue=").append(afterClockValue)
                        .append(", usedPreviousValues=").append(usedPreviousValues)
                        .append(", notificationAwareExtraChanged=").append(true)
                        .endl();
            }
            return false;
        }
        return super.snapshotCompletedConsistently(afterClockValue, usedPreviousValues);
    }

    /**
     * A snapshot that used previous values cannot read a {@link NotificationAwareDependency} consistently if that
     * dependency changes its guarded state while being snapshotted. These dependencies cannot provide a consistent set
     * of previous values, so a snapshot that used previous values must be retried.
     *
     * @param clockValue The clock value to evaluate against
     * @param usedPreviousValues Whether the snapshot used previous values
     * @return Whether the notification aware extras were read consistently
     */
    private boolean notificationAwareExtrasConsistent(final long clockValue, final boolean usedPreviousValues) {
        if (!usedPreviousValues || notificationAwareExtras.length == 0) {
            return true;
        }
        final long step = LogicalClock.getStep(clockValue);
        for (final NotificationAwareDependency extra : notificationAwareExtras) {
            if (extra.stateChangedOnStep(step)) {
                return false;
            }
        }
        return true;
    }

    private static boolean satisfied(@NotNull final NotificationQueue.Dependency dependency, final long step) {
        if (dependency instanceof NotificationStepSource
                && ((NotificationStepSource) dependency).getLastNotificationStep() == step) {
            return true;
        }
        return dependency.satisfied(step);
    }
}
