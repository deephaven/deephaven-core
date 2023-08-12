/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph;

import io.deephaven.util.annotations.FinalDefault;

/**
 * A logical update clock interface that has two states, Updating and Idle.
 */
public interface LogicalClock {

    /**
     * The "null" value, which encodes {step=-1, state=Idle}. Used as a marker when no clock value is appropriate, e.g.
     * for snapshots of static data.
     */
    long NULL_CLOCK_VALUE = -1L;

    /**
     * The state component of a logical timestamp.
     */
    enum State {

        /**
         * Clock state for logical timestamps when the associated {@link UpdateGraph} is propagating updates.
         */
        Updating,

        /**
         * Clock state for logical timestamps when the associated {@link UpdateGraph} is <em>not</em> propagating
         * updates.
         */
        Idle
    }

    long STEP_SHIFT = 1;
    long STATE_MASK = 1L;

    /**
     * Get the clock step for the input clock value. The step increments one time for each complete
     * {@link LogicalClockImpl#startUpdateCycle() start} - {@link LogicalClockImpl#completeUpdateCycle() end} cycle.
     *
     * @param value The clock value to get the step for
     * @return The clock step associated with value
     */
    static long getStep(final long value) {
        return value >>> STEP_SHIFT;
    }

    /**
     * Get the {@link State} of the LogicalClock for a particular clock value.
     *
     * @param value The clock value
     * @return The clock state associated with the input value
     */
    static State getState(final long value) {
        return ((value & STATE_MASK) == 0) ? State.Updating : State.Idle;
    }

    /**
     * Get the current value of the clock.
     */
    long currentValue();

    /**
     * Get the current Step of the clock.
     *
     * @see #getStep(long)
     */
    @FinalDefault
    default long currentStep() {
        return getStep(currentValue());
    }

    /**
     * Get the current clock state.
     *
     * @see #getState(long)
     */
    @FinalDefault
    default State currentState() {
        return getState(currentValue());
    }
}
