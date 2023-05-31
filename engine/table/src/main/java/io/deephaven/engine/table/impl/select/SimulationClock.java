/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.DateTime;
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

    private final DateTime endTime;
    private final long stepNanos;

    private final Runnable refreshTask = this::advance; // Save this in a reference so we can deregister it.

    private enum State {
        NOT_STARTED, STARTED, DONE
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
    private final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();

    private final Condition ugpCondition = updateGraph.exclusiveLock().newCondition();

    private DateTime now;

    /**
     * Create a simulation clock for the specified time range and step.
     * 
     * @param startTime The initial time that will be returned by this clock, before it is started
     * @param endTime The final time that will be returned by this clock, when the simulation has completed
     * @param stepSize The time to "elapse" in each run loop
     */
    public SimulationClock(@NotNull final String startTime,
            @NotNull final String endTime,
            @NotNull final String stepSize) {
        this(DateTimeUtils.convertDateTime(startTime), DateTimeUtils.convertDateTime(endTime),
                DateTimeUtils.convertTime(stepSize));
    }

    /**
     * Create a simulation clock for the specified time range and step.
     *
     * @param startTime The initial time that will be returned by this clock, before it is started
     * @param endTime The final time that will be returned by this clock, when the simulation has completed
     * @param stepNanos The number of nanoseconds to "elapse" in each run loop
     */
    public SimulationClock(@NotNull final DateTime startTime,
            @NotNull final DateTime endTime,
            final long stepNanos) {
        Require.neqNull(startTime, "startTime");
        this.endTime = Require.neqNull(endTime, "endTime");
        Require.requirement(DateTimeUtils.isBefore(startTime, endTime), "DateTimeUtils.isBefore(startTime, endTime)");
        this.stepNanos = Require.gtZero(stepNanos, "stepNanos");
        now = startTime;
    }

    @Override
    public long currentTimeMillis() {
        return now.getMillis();
    }

    @Override
    public long currentTimeMicros() {
        return now.getMicros();
    }

    @Override
    public long currentTimeNanos() {
        return now.getNanos();
    }

    @Override
    public Instant instantNanos() {
        return now.getInstant();
    }

    @Override
    public Instant instantMillis() {
        return now.getInstant();
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
        if (maxSpeed) {
            updateGraph.setTargetCycleDurationMillis(0);
        }
        if (!state.compareAndSet(State.NOT_STARTED, State.STARTED)) {
            throw new IllegalStateException(this + " already started");
        }
        updateGraph.addSource(refreshTask);
    }

    /**
     * Advance the simulation. Public access for unit tests.
     */
    @VisibleForTesting
    public void advance() {
        Assert.eq(state.get(), "state.get()", State.STARTED);
        if (now.getNanos() == endTime.getNanos()) {
            Assert.assertion(state.compareAndSet(State.STARTED, State.DONE),
                    "state.compareAndSet(State.STARTED, State.DONE)");
            updateGraph.removeSource(refreshTask);
            updateGraph.requestSignal(ugpCondition);
            return; // This return is not strictly necessary, but it seems clearer this way.
        }
        final DateTime incremented = DateTimeUtils.plus(now, stepNanos);
        now = DateTimeUtils.isAfter(incremented, endTime) ? endTime : incremented;
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
