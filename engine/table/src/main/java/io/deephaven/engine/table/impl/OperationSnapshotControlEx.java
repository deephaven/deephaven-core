//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.updategraph.ClockInconsistencyException;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.WaitNotification;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Variant of {@link OperationSnapshotControl} that considers "extra" {@link NotificationQueue.Dependency dependencies}
 * in addition to the source {@link BaseTable} when determining whether to use previous values during initialization or
 * evaluating success. This is useful anytime an operation needs to listen to and snapshot one data source while also
 * snapshotting others.
 * <p>
 * The decision is made over the source and every extra together. Previous values are used only when none of them has
 * been satisfied on the step. If some are satisfied and others are not, the control waits for the rest and then uses
 * current values; once all are satisfied it uses current values without waiting. A static source cannot be unsatisfied,
 * so it does not count.
 * <p>
 * For the consistency check afterwards, extras are treated as <em>notification oblivious</em> by default: their
 * notifications do not invalidate a snapshot, because their previous values remain stable for the whole cycle and a
 * snapshot using previous values reads them consistently no matter when they tick. That is the correct treatment for
 * the usual extras, which are tables or table-backed structures such as data index tables.
 * <p>
 * An extra that also implements {@link NotificationAwareDependency} is the exception: it keeps no previous version of
 * the state it guards, so a snapshot that used previous values while that extra changed its state on the same step must
 * be retried. Such extras are detected automatically from those passed to the constructor, so callers need do nothing
 * beyond passing them as dependencies.
 * <p>
 * On an update-processing thread a snapshot never uses previous values and cannot wait, so any unsatisfied dependency
 * is an {@link IllegalStateException}: the operation must be ordered after that dependency by the caller.
 */
public final class OperationSnapshotControlEx extends OperationSnapshotControl {

    private static final Logger log = LoggerFactory.getLogger(OperationSnapshotControlEx.class);

    private final NotificationQueue.Dependency[] extras;
    private final NotificationAwareDependency[] notificationAwareExtras;
    private final long[] notificationAwareChangeSteps;
    private int subscribedExtras;

    public OperationSnapshotControlEx(
            @NotNull final BaseTable<?> sourceTable,
            @NotNull final NotificationQueue.Dependency... extras) {
        super(sourceTable);
        this.extras = extras;
        this.notificationAwareExtras = Arrays.stream(extras)
                .filter(NotificationAwareDependency.class::isInstance)
                .map(NotificationAwareDependency.class::cast)
                .toArray(NotificationAwareDependency[]::new);
        this.notificationAwareChangeSteps = new long[notificationAwareExtras.length];
    }

    @Override
    @SuppressWarnings("AutoBoxing")
    public synchronized Boolean usePreviousValues(final long beforeClockValue) {
        recordDependencyState();

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

        final boolean sourceSatisfied;
        final NotificationQueue.Dependency[] extrasNotSatisfied;
        try {
            sourceSatisfied = satisfied(sourceTable, beforeStep);
            extrasNotSatisfied = Arrays.stream(extras)
                    .sequential()
                    .filter(extra -> !satisfied(extra, beforeStep))
                    .toArray(NotificationQueue.Dependency[]::new);
        } catch (ClockInconsistencyException e) {
            return null;
        }

        final boolean sourceUpdated = sourceTable.isRefreshing() && sourceSatisfied;
        final boolean nothingUpdated = !sourceUpdated && extrasNotSatisfied.length == extras.length;

        final Boolean usePrev;
        if (sourceSatisfied && extrasNotSatisfied.length == 0) {
            usePrev = false;
        } else if (getUpdateGraph().currentThreadProcessesUpdates()) {
            throw new IllegalStateException(String.format(
                    "Cannot snapshot from an update-processing thread with unsatisfied dependencies %s: "
                            + "the operation must declare a dependency on them",
                    describe(notYetSatisfied(sourceSatisfied, extrasNotSatisfied))));
        } else if (nothingUpdated) {
            usePrev = true;
        } else {
            // Partially satisfied. The wait cannot time out; it is refused if the step's updating phase has ended.
            final boolean waitSuccessful = WaitNotification.waitForSatisfaction(beforeStep,
                    notYetSatisfied(sourceSatisfied, extrasNotSatisfied));
            if (waitSuccessful) {
                usePrev = false;
            } else if (getUpdateGraph().clock().currentStep() == beforeStep) {
                // Refused on the same step: the step must have completed, so every dependency is satisfied for it.
                usePrev = false;
            } else {
                // Refused and a later step has begun: this attempt cannot be judged, the caller retries.
                usePrev = null;
            }
        }

        if (usePrev != null && usePrev == false) {
            // Everything is satisfied for this step, record how it is *now* rather than as it was before the wait.
            recordDependencyState();
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
                    .append(", sourceSatisfied=").append(sourceSatisfied)
                    .append(", extrasNotSatisfied=").append(Arrays.toString(extrasNotSatisfied))
                    .append(", usePrev=").append(usePrev)
                    .endl();
        }
        return usePrev;
    }

    /**
     * Record the source's last notification step and each aware extra's last state change step.
     */
    private void recordDependencyState() {
        lastNotificationStep = sourceTable.getLastNotificationStep();
        for (int ei = 0; ei < notificationAwareExtras.length; ++ei) {
            notificationAwareChangeSteps[ei] = notificationAwareExtras[ei].lastStateChangeStep();
        }
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
            if (extra.lastStateChangeStep() == step) {
                return false;
            }
        }
        return true;
    }

    /**
     * Subscribe every aware extra, each requiring the state it guards to be as this attempt found it. One that refuses,
     * or that has failed, undoes those already subscribed: the snapshot attempt will be aborted.
     * <p>
     * Only an aware extra has anything to subscribe to, because only it pushes its state changes into this operation.
     * An oblivious extra's previous values are stable for the whole cycle, so there is no change for this attempt to
     * have missed: what such an extra describes reaches the operation through the listener chain. Ordering is not a
     * subscription either; the merged listener is built with the dependencies it must run after.
     * <p>
     * This is not the check {@link #notificationAwareExtrasConsistent} makes. That one refuses a previous-values
     * attempt that read a change made on its own step, whenever that change was made; this one refuses any attempt that
     * would begin following an extra having missed a change made since the attempt began.
     */
    @Override
    boolean maybeSubscribeDependencies() {
        for (int ei = 0; ei < notificationAwareExtras.length; ++ei) {
            final boolean subscribed;
            try {
                subscribed = notificationAwareExtras[ei].subscribe(notificationAwareChangeSteps[ei]);
            } catch (Exception e) {
                maybeUnsubscribeDependencies();
                throw e;
            }
            if (!subscribed) {
                maybeUnsubscribeDependencies();
                if (DEBUG) {
                    log.info().append("OperationSnapshotControlEx {source=")
                            .append(System.identityHashCode(sourceTable))
                            .append(", control=").append(System.identityHashCode(this))
                            .append("} maybeSubscribeDependencies: refused by extra=")
                            .append(System.identityHashCode(notificationAwareExtras[ei]))
                            .endl();
                }
                return false;
            }
            subscribedExtras = ei + 1;
        }
        return true;
    }

    @Override
    void maybeUnsubscribeDependencies() {
        while (subscribedExtras > 0) {
            notificationAwareExtras[--subscribedExtras].unsubscribe();
        }
    }

    private NotificationQueue.Dependency[] notYetSatisfied(
            final boolean sourceSatisfied,
            @NotNull final NotificationQueue.Dependency[] extrasNotSatisfied) {
        return Stream.concat(
                sourceSatisfied ? Stream.empty() : Stream.of(sourceTable),
                Arrays.stream(extrasNotSatisfied))
                .toArray(NotificationQueue.Dependency[]::new);
    }

    private static String describe(@NotNull final NotificationQueue.Dependency[] dependencies) {
        return Arrays.stream(dependencies)
                .map(dependency -> new LogOutputStringImpl().append(dependency).toString())
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private static boolean satisfied(@NotNull final NotificationQueue.Dependency dependency, final long step) {
        if (dependency instanceof NotificationStepSource
                && ((NotificationStepSource) dependency).getLastNotificationStep() == step) {
            return true;
        }
        return dependency.satisfied(step);
    }
}
