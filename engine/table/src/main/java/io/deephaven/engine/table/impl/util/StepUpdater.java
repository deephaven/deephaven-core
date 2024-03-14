//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.updategraph.ClockInconsistencyException;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Tool for maintaining recorded clock steps.
 */
public class StepUpdater {

    /**
     * Validated that {@code requestedStep} is greater than or equal to {@code recordedStep}.
     *
     * @param requestedStep The requested step, e.g. as an argument to
     *        {@link io.deephaven.engine.updategraph.NotificationQueue.Dependency#satisfied(long) Dependency.satisfied}
     * @param recordedStep The highest recorded step
     * @throws ClockInconsistencyException if {@code requestedStep < recordedStep}
     */
    public static void checkForOlderStep(final long requestedStep, final long recordedStep) {
        if (requestedStep < recordedStep) {
            throw new ClockInconsistencyException(String.format(
                    "Requested step %s is less than highest recorded step %s", requestedStep, recordedStep));
        }
    }

    /**
     * Update a recorded step field to be at least {@code step}.
     *
     * @param recordedStepUpdater An updater for the recorded step field
     * @param instance The instance to update
     * @param step The target step value to record
     * @param <T> The type of {@code instance} expected by {@code recordedStepUpdater}
     * @throws ClockInconsistencyException if {@code step < recordedStepUpdater.get(instance)}
     */
    public static <T> void tryUpdateRecordedStep(
            @NotNull final AtomicLongFieldUpdater<T> recordedStepUpdater,
            @NotNull final T instance,
            final long step) {
        long oldRecordedStep;
        while ((oldRecordedStep = recordedStepUpdater.get(instance)) < step) {
            if (recordedStepUpdater.compareAndSet(instance, oldRecordedStep, step)) {
                return;
            }
        }
        checkForOlderStep(step, oldRecordedStep);
    }

    /**
     * Update a recorded step field to be exactly {@code step}. Validate that the previous recorded step was less than
     * {@code step}.
     *
     * @param recordedStepUpdater An updater for the recorded step field
     * @param instance The instance to update
     * @param step The target step value to record
     * @param <T> The type of {@code instance} expected by {@code recordedStepUpdater}
     */
    public static <T> void forceUpdateRecordedStep(
            @NotNull final AtomicLongFieldUpdater<T> recordedStepUpdater,
            @NotNull final T instance,
            final long step) {
        final long oldRecordedStep = recordedStepUpdater.getAndSet(instance, step);
        Assert.lt(oldRecordedStep, "oldRecordedStep", step, "step");
    }
}
