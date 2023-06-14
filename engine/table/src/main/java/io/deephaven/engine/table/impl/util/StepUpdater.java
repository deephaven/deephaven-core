package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Tool for maintaining recorded clock steps.
 */
public class StepUpdater {

    /**
     * Update a recorded step field to be at least {@code step}.
     *
     * @param recordedStepUpdater An updater for the recorded step field
     * @param instance The instance to update
     * @param step The target step value to record
     * @param <T> The type of {@code instance} expected by {@code recordedStepUpdater}
     */
    public static <T> void tryUpdateRecordedStep(
            @NotNull final AtomicLongFieldUpdater<T> recordedStepUpdater,
            @NotNull final T instance,
            final long step) {
        long oldRecordedStep;
        while ((oldRecordedStep = recordedStepUpdater.get(instance)) < step) {
            if (recordedStepUpdater.compareAndSet(instance, oldRecordedStep, step)) {
                break;
            }
        }
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
