//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;

/**
 * Clock implementation for use with ClockFilter implementations to advance simulation time.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class SimulationClock implements Clock {

    private final Instant endTime;
    private final long stepNanos;

    private final Runnable refreshTask = this::advance; // Save this in a reference so we can deregister it.

    private enum State {
        NOT_STARTED, STARTED, DONE
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
    private final PeriodicUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

    private final Condition ugpCondition = updateGraph.exclusiveLock().newCondition();

    private Instant now;
    private boolean maxSpeed;

    /**
     * Create a simulation clock for the specified time range and step.
     *
     * @param startTime The initial time that will be returned by this clock, before it is started
     * @param endTime The final time that will be returned by this clock, when the simulation has completed
     * @param stepSize The time to "elapse" in each run loop
     */
    public SimulationClock(
            @NotNull final String startTime,
            @NotNull final String endTime,
            @NotNull final String stepSize) {
        this(DateTimeUtils.parseInstant(startTime), DateTimeUtils.parseInstant(endTime),
                DateTimeUtils.parseDurationNanos(stepSize));
    }

    /**
     * Create a simulation clock for the specified time range and step.
     *
     * @param startTime The initial time that will be returned by this clock, before it is started
     * @param endTime The final time that will be returned by this clock, when the simulation has completed
     * @param stepNanos The number of nanoseconds to "elapse" in each run loop
     */
    public SimulationClock(
            @NotNull final Instant startTime,
            @NotNull final Instant endTime,
            final long stepNanos) {
        Require.neqNull(startTime, "startTime");
        this.endTime = Require.neqNull(endTime, "endTime");
        Require.requirement(DateTimeUtils.isBefore(startTime, endTime), "DateTimeUtils.isBefore(startTime, endTime)");
        this.stepNanos = Require.gtZero(stepNanos, "stepNanos");
        now = startTime;
    }

    @Override
    public long currentTimeMillis() {
        return DateTimeUtils.epochMillis(now);
    }

    @Override
    public long currentTimeMicros() {
        return DateTimeUtils.epochMicros(now);
    }

    @Override
    public long currentTimeNanos() {
        return DateTimeUtils.epochNanos(now);
    }

    @Override
    public Instant instantNanos() {
        return now;
    }

    @Override
    public Instant instantMillis() {
        return now;
    }

    /**
     * Start the simulation.
     */
    public void start() {
        start(true);
    }

    /**
     * Start the simulation.
     *
     * @param maxSpeed run the simulation clock at the max possible speed.
     */
    public void start(final boolean maxSpeed) {
        if (!state.compareAndSet(State.NOT_STARTED, State.STARTED)) {
            throw new IllegalStateException(this + " already started");
        }
        this.maxSpeed = maxSpeed;
        updateGraph.addSource(refreshTask);
        if (maxSpeed) {
            updateGraph.requestRefresh();
        }
    }

    /**
     * Advance the simulation. Public access for unit tests.
     */
    @VisibleForTesting
    public void advance() {
        Assert.eq(state.get(), "state.get()", State.STARTED);
        if (DateTimeUtils.epochNanos(now) == DateTimeUtils.epochNanos(endTime)) {
            Assert.assertion(state.compareAndSet(State.STARTED, State.DONE),
                    "state.compareAndSet(State.STARTED, State.DONE)");
            updateGraph.removeSource(refreshTask);
            updateGraph.requestSignal(ugpCondition);
            return; // This return is not strictly necessary, but it seems clearer this way.
        }
        final Instant incremented = DateTimeUtils.plus(now, stepNanos);
        now = DateTimeUtils.isAfter(incremented, endTime) ? endTime : incremented;
        if (maxSpeed) {
            updateGraph.requestRefresh();
        }
    }

    /**
     * Is the simulation done?
     *
     * @return True if the simulation is done
     */
    public boolean done() {
        return state.get() == State.DONE;
    }

    /**
     * Wait for the simulation to be done, without ever throwing InterruptedException.
     */
    public void awaitDoneUninterruptibly() {
        while (!done()) {
            updateGraph.exclusiveLock().doLocked(ugpCondition::awaitUninterruptibly);
        }
    }

    /**
     * Wait for the simulation to be done.
     */
    public void awaitDone() throws InterruptedException {
        while (!done()) {
            updateGraph.exclusiveLock().doLocked(ugpCondition::await);
        }
    }
}
