package io.deephaven.db.v2.select;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;

/**
 * Clock implementation for use with ClockFilter implementations to advance simulation time.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class SimulationClock implements Clock {

    private final DBDateTime endTime;
    private final long stepNanos;

    private final LiveTable refreshTask = this::advance; // Save this in a reference so we can
                                                         // deregister it.

    private enum State {
        NOT_STARTED, STARTED, DONE
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
    private final Condition ltmCondition = LiveTableMonitor.DEFAULT.exclusiveLock().newCondition();

    private DBDateTime now;

    /**
     * Create a simulation clock for the specified time range and step.
     * 
     * @param startTime The initial time that will be returned by this clock, before it is started
     * @param endTime The final time that will be returned by this clock, when the simulation has
     *        completed
     * @param stepSize The time to "elapse" in each refresh loop
     */
    public SimulationClock(@NotNull final String startTime,
        @NotNull final String endTime,
        @NotNull final String stepSize) {
        this(DBTimeUtils.convertDateTime(startTime), DBTimeUtils.convertDateTime(endTime),
            DBTimeUtils.convertTime(stepSize));
    }

    /**
     * Create a simulation clock for the specified time range and step.
     *
     * @param startTime The initial time that will be returned by this clock, before it is started
     * @param endTime The final time that will be returned by this clock, when the simulation has
     *        completed
     * @param stepNanos The number of nanoseconds to "elapse" in each refresh loop
     */
    public SimulationClock(@NotNull final DBDateTime startTime,
        @NotNull final DBDateTime endTime,
        final long stepNanos) {
        Require.neqNull(startTime, "startTime");
        this.endTime = Require.neqNull(endTime, "endTime");
        Require.requirement(DBTimeUtils.isBefore(startTime, endTime),
            "DBTimeUtils.isBefore(startTime, endTime)");
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
            LiveTableMonitor.DEFAULT.setTargetCycleTime(0);
        }
        if (!state.compareAndSet(State.NOT_STARTED, State.STARTED)) {
            throw new IllegalStateException(this + " already started");
        }
        LiveTableMonitor.DEFAULT.addTable(refreshTask);
    }

    /**
     * Advance the simulation. Package access for unit tests.
     */
    void advance() {
        Assert.eq(state.get(), "state.get()", State.STARTED);
        if (now.getNanos() == endTime.getNanos()) {
            Assert.assertion(state.compareAndSet(State.STARTED, State.DONE),
                "state.compareAndSet(State.STARTED, State.DONE)");
            LiveTableMonitor.DEFAULT.removeTable(refreshTask);
            LiveTableMonitor.DEFAULT.requestSignal(ltmCondition);
            return; // This return is not strictly necessary, but it seems clearer this way.
        }
        final DBDateTime incremented = DBTimeUtils.plus(now, stepNanos);
        now = DBTimeUtils.isAfter(incremented, endTime) ? endTime : incremented;
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
            LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(ltmCondition::awaitUninterruptibly);
        }
    }

    /**
     * Wait for the simulation to be done.
     */
    public void awaitDone() throws InterruptedException {
        while (!done()) {
            LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(ltmCondition::await);
        }
    }
}
