/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.annotations.TestUseOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * A logical update clock that has two states, Updating and Idle.
 * </p>
 *
 * <p>
 * Each time {@link #startUpdateCycle()} is called, the clock transitions to the Updating state and
 * the current {@link #currentValue() value} is incremented by one.
 * </p>
 *
 * <p>
 * When {@link #completeUpdateCycle()} is called, the clock transitions back to Idle.
 * </p>
 */
public enum LogicalClock {

    DEFAULT;

    /**
     * The state component of a logical timestamp.
     */
    public enum State {

        /**
         * Clock state for logical timestamps when the associated
         * {@link io.deephaven.db.tables.live.LiveTableMonitor} is propagating updates.
         */
        Updating,

        /**
         * Clock state for logical timestamps when the associated
         * {@link io.deephaven.db.tables.live.LiveTableMonitor} is <em>not</em> propagating updates.
         */
        Idle
    }

    private static final long STEP_SHIFT = 1;
    private static final long STATE_MASK = 1L;

    // {2, Idle}, just in case any code has 0 or 1 as an initializer.
    private final AtomicLong currentValue = new AtomicLong(5L);

    /**
     * Get the clock step for the input clock value. The step increments one time for each complete
     * {@link #startUpdateCycle() start} - {@link #completeUpdateCycle() end} cycle.
     *
     * @param value The clock value to get the step for
     * @return The clock step associated with value
     */
    public static long getStep(final long value) {
        return value >>> STEP_SHIFT;
    }

    /**
     * Get the {@link State} of the LogicalClock for a particular clock value.
     *
     * @param value The clock value
     * @return The clock state associated with the input value
     */
    public static State getState(final long value) {
        return ((value & STATE_MASK) == 0) ? State.Updating : State.Idle;
    }

    /**
     * Get the current value of the clock.
     */
    public final long currentValue() {
        return currentValue.get();
    }

    /**
     * Get the current Step of the clock.
     *
     * @see #getStep(long)
     */
    public final long currentStep() {
        return getStep(currentValue());
    }

    /**
     * Get the current clock state.
     *
     * @see #getState(long)
     */
    public final State currentState() {
        return getState(currentValue());
    }

    /**
     * Increment the current value and set the clock state to {@link State#Updating updating}.
     *
     * @implNote The clock must have been {@link State#Idle idle} before this method is called.
     */
    public final long startUpdateCycle() {
        final long beforeValue = currentValue.get();
        Assert.eq(getState(beforeValue), "getState(beforeValue)", State.Idle);
        final long afterValue = currentValue.incrementAndGet();
        Assert.eq(afterValue, "currentValue.incrementAndGet()", beforeValue + 1, "beforeValue + 1");
        return afterValue;
    }

    /**
     * Increment the current step and set the clock state to {@link State#Idle idle}.
     *
     * @implNote The clock must have been {@link State#Updating updating} before this method is
     *           called.
     */
    public final void completeUpdateCycle() {
        final long value = currentValue.get();
        Assert.eq(getState(value), "getState(value)", State.Updating);
        Assert.eq(currentValue.incrementAndGet(), "currentValue.incrementAndGet()", value + 1,
            "value + 1");
    }

    /**
     * After we complete a table refresh, we must ensure that the logical clock is idle.
     *
     * <p>
     * The only valid possibilities are (1) we have completed the cycle, in which case we return; or
     * (2) we have terminated the cycle early and have the same value as at the start of our
     * updating cycle, in which case we complete the cycle.
     * </p>
     *
     * <p>
     * If our clock is any other value; then it was changed out from under us and we throw an
     * exception.
     * </p>
     *
     * @param updatingCycleValue the clock value at the end of {@link #startUpdateCycle}
     */
    public final void ensureUpdateCycleCompleted(final long updatingCycleValue) {
        final long value = currentValue.get();
        if (value == updatingCycleValue + 1) {
            return;
        }
        if (value == updatingCycleValue) {
            ProcessEnvironment.getDefaultLog(LogicalClock.class).warn()
                .append("LogicalClock cycle was not completed in normal operation, value=")
                .append(value).endl();
            completeUpdateCycle();
            return;
        }
        throw new IllegalStateException("Inconsistent LogicalClock value at end of cycle, expected "
            + (updatingCycleValue + 1) + ", encountered " + value);
    }

    /**
     * Reset the clock to its initial state, in order to ensure that unit tests proceed cleanly.
     */
    @TestUseOnly
    public final void resetForUnitTests() {
        currentValue.set(5L);
    }
}
