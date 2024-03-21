//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.base.verify.Assert;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.process.ProcessEnvironment;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * A logical update clock that has two states, Updating and Idle.
 * </p>
 *
 * <p>
 * Each time {@link #startUpdateCycle()} is called, the clock transitions to the Updating state and the current
 * {@link #currentValue() value} is incremented by one.
 * </p>
 *
 * <p>
 * When {@link #completeUpdateCycle()} is called, the clock transitions back to Idle.
 * </p>
 */
public class LogicalClockImpl implements LogicalClock {

    private static final Logger log = LoggerFactory.getLogger(LogicalClockImpl.class);

    /**
     * Our initial clock value. Equivalent to {step=2, state=Idle}. Uses step 2 in case any code has 0 or 1 as an
     * initializer.
     */
    private static final long INITIAL_CLOCK_VALUE = 5L;

    /**
     * The current value, encoding both step and state as a single long value.
     */
    private final AtomicLong currentValue = new AtomicLong(INITIAL_CLOCK_VALUE);

    /**
     * Get the current value of the clock.
     */
    @Override
    public final long currentValue() {
        return currentValue.get();
    }


    /**
     * Increment the current value and set the clock state to {@link State#Updating updating}.
     *
     * @implNote The clock must have been {@link State#Idle idle} before this method is called.
     */
    public final long startUpdateCycle() {
        final long beforeValue = currentValue.get();
        if (beforeValue == Long.MAX_VALUE) {
            ProcessEnvironment.get().getFatalErrorReporter().report("Maximum logical clock cycles exceeded");
        }
        Assert.eq(LogicalClock.getState(beforeValue), "getState(beforeValue)", State.Idle);
        final long afterValue = currentValue.incrementAndGet();
        Assert.eq(afterValue, "currentValue.incrementAndGet()", beforeValue + 1, "beforeValue + 1");
        return afterValue;
    }

    /**
     * Increment the current step and set the clock state to {@link State#Idle idle}.
     *
     * @implNote The clock must have been {@link State#Updating updating} before this method is called.
     */
    public final void completeUpdateCycle() {
        final long value = currentValue.get();
        // If we try to exceed our maximum clock value, it will be on an Idle to Updating transition, since
        // Long.MAX_VALUE & STATE_MASK == 1, which means that the maximum value occurs upon reaching an Idle phase.
        Assert.eq(LogicalClock.getState(value), "getState(value)", State.Updating);
        Assert.eq(currentValue.incrementAndGet(), "currentValue.incrementAndGet()", value + 1, "value + 1");
    }

    /**
     * After we complete a table run, we must ensure that the logical clock is idle.
     *
     * <p>
     * The only valid possibilities are (1) we have completed the cycle, in which case we return; or (2) we have
     * terminated the cycle early and have the same value as at the start of our updating cycle, in which case we
     * complete the cycle.
     * </p>
     *
     * <p>
     * If our clock is any other value; then it was changed out from under us and we throw an exception.
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
            log.warn()
                    .append("LogicalClockImpl cycle was not completed in normal operation, value=").append(value)
                    .endl();
            completeUpdateCycle();
            return;
        }
        throw new IllegalStateException("Inconsistent LogicalClockImpl value at end of cycle, expected "
                + (updatingCycleValue + 1) + ", encountered " + value);
    }

    /**
     * Reset the clock to its initial state, in order to ensure that unit tests proceed cleanly.
     */
    @TestUseOnly
    public final void resetForUnitTests() {
        currentValue.set(INITIAL_CLOCK_VALUE);
    }
}
