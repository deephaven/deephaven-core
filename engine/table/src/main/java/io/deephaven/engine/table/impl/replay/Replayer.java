//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.replay;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.InstrumentedUpdateSource;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;

import java.time.Instant;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * Replay historical data as simulated real-time data.
 */
public class Replayer implements ReplayerInterface, Runnable {
    private static final Logger log = LoggerFactory.getLogger(Replayer.class);

    protected Instant startTime;
    protected Instant endTime;
    private long deltaNanos = Long.MAX_VALUE;
    private CopyOnWriteArrayList<ReplayTableBase> currentTables = new CopyOnWriteArrayList<>();
    private volatile boolean done;
    private boolean lastLap;

    private final ReplayerHandle handle = () -> Replayer.this;

    private final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();
    private final SourceRefresher sourceRefresher = new SourceRefresher();

    // Condition variable for use with PeriodicUpdateGraph lock - the object monitor is no longer used
    private final Condition ugpCondition = updateGraph.exclusiveLock().newCondition();

    /**
     * Creates a new replayer.
     *
     * @param startTime start time
     * @param endTime end time
     */
    public Replayer(Instant startTime, Instant endTime) {
        this.endTime = endTime;
        this.startTime = startTime;
    }

    /**
     * Starts replaying data.
     */
    @Override
    public void start() {
        deltaNanos = DateTimeUtils.millisToNanos(System.currentTimeMillis()) - DateTimeUtils.epochNanos(startTime);
        // Note: ReplayTables are allowed to run in parallel with this Replayer. However, we may wish to use an
        // UpdateSourceCombiner to ensure that all of these tables are refreshed as a unit.
        updateGraph.addSource(sourceRefresher);
        for (ReplayTableBase currentTable : currentTables) {
            currentTable.start();
        }
    }

    /**
     * Has the replayer finished replaying all data.
     *
     * @return true if the replayer has finished replaying all data; false otherwise.
     */
    @Override
    public boolean isDone() {
        return done;
    }

    /**
     * Shuts down the replayer.
     */
    @Override
    public void shutdown() {
        Clock clock = clock();
        endTime = clock.instantNanos();
        if (done) {
            return;
        }
        updateGraph.removeSource(sourceRefresher);
        for (ReplayTableBase currentTable : currentTables) {
            currentTable.stop();
        }
        currentTables = null;
        if (updateGraph.exclusiveLock().isHeldByCurrentThread()) {
            shutdownInternal();
        } else if (updateGraph.currentThreadProcessesUpdates()) {
            updateGraph.addNotification(new TerminalNotification() {
                @Override
                public boolean mustExecuteWithUpdateGraphLock() {
                    return true;
                }

                @Override
                public void run() {
                    shutdownInternal();
                }
            });
        } else {
            updateGraph.exclusiveLock().doLocked(this::shutdownInternal);
        }
    }

    private void shutdownInternal() {
        Assert.assertion(updateGraph.exclusiveLock().isHeldByCurrentThread(),
                "updateGraph.exclusiveLock().isHeldByCurrentThread()");
        done = true;
        ugpCondition.signalAll();
    }

    /**
     * Wait a specified interval for the replayer to complete. If the replayer has not completed by the end of the
     * interval, the method returns.
     *
     * @param maxTimeMillis maximum number of milliseconds to wait.
     * @throws CancellationException thread was interrupted.
     */
    @Override
    public void waitDone(long maxTimeMillis) {
        long expiryTime = System.currentTimeMillis() + maxTimeMillis;
        if (done) {
            return;
        }
        updateGraph.exclusiveLock().doLocked(() -> {
            while (!done && expiryTime > System.currentTimeMillis()) {
                try {
                    ugpCondition.await(expiryTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException interruptIsCancel) {
                    throw new CancellationException("Interrupt detected", interruptIsCancel);
                }
            }
        });
    }

    /**
     * Schedule a task to execute.
     *
     * @param task task to execute
     * @param delayMillis delay in milliseconds before first executing the task
     * @param periodMillis frequency in milliseconds to execute the task.
     */
    @Override
    public void schedule(TimerTask task, long delayMillis, long periodMillis) {
        timerTasks.add(new PeriodicTask(task, delayMillis, periodMillis));
    }

    /**
     * Gets a time provider for the replayer. The time provider returns the current replay time.
     *
     * @param replayer replayer
     * @return time provider that returns the current replay time.
     */
    public static Clock getClock(final ReplayerInterface replayer) {
        return replayer == null ? DateTimeUtils.currentClock() : replayer.clock();
    }

    /**
     * Sets the current replay time.
     *
     * @param updatedTime new replay time.
     */
    @Override
    public void setTime(long updatedTime) {
        if (deltaNanos == Long.MAX_VALUE) {
            startTime = DateTimeUtils.epochMillisToInstant(updatedTime);
        } else {
            long adjustment = updatedTime - clock().currentTimeMillis();
            if (adjustment > 0) {
                deltaNanos = deltaNanos - DateTimeUtils.millisToNanos(adjustment);
            }
        }
    }

    /**
     * Prepares a historical table for replaying.
     *
     * @param dataSource historical table to replay
     * @param timeColumn column in the table containing timestamps
     * @return dynamic, replayed version of the table.
     */
    @Override
    public Table replay(Table dataSource, String timeColumn) {
        if (dataSource.isRefreshing()) {
            dataSource = dataSource.snapshot();
        }
        final ReplayTable result;
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            result = new ReplayTable(dataSource.getRowSet(), dataSource.getColumnSourceMap(), timeColumn, this);
        }
        currentTables.add(result);
        if (deltaNanos < Long.MAX_VALUE) {
            result.start();
        }
        return result;
    }

    /**
     * Prepares a grouped historical table for replaying. This method can be faster than the ungrouped replay, but the
     * performance increase comes with a cost. Within a group, the data ordering is maintained. Between groups, data
     * ordering is not maintained for a time interval.
     *
     * @param dataSource historical table to replay
     * @param timeColumn column in the table containing timestamps
     * @return dynamic, replayed version of the table.
     */
    @Override
    public Table replayGrouped(Table dataSource, String timeColumn, String groupingColumn) {
        if (dataSource.isRefreshing()) {
            dataSource = dataSource.snapshot();
        }
        final ReplayGroupedFullTable result;
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            result = new ReplayGroupedFullTable(dataSource, timeColumn, this, groupingColumn);
        }
        currentTables.add(result);
        if (deltaNanos < Long.MAX_VALUE) {
            result.start();
        }
        return result;
    }

    /**
     * Prepares a grouped historical table for replaying as a last-by table.
     *
     * @param dataSource historical table to replay
     * @param timeColumn column in the table containing timestamps
     * @param groupingColumns columns used as the key in computing last-by
     * @return dynamic, replayed version of the last-by table.
     */
    @Override
    public Table replayGroupedLastBy(Table dataSource, String timeColumn, String... groupingColumns) {
        if (dataSource.isRefreshing()) {
            dataSource = dataSource.snapshot();
        }
        final ReplayLastByGroupedTable result;
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            result = new ReplayLastByGroupedTable(dataSource, timeColumn, this, groupingColumns);
        }
        currentTables.add(result);
        if (deltaNanos < Long.MAX_VALUE) {
            result.start();
        }
        return result;
    }

    /**
     * Register the time column and row set from a new table to replay. Most users will use <code>replay</code>,
     * <code>replayGrouped</code>, or <code>replayGroupedLastBy</code> instead of this function.
     *
     * @param rowSet table row set
     * @param timestampSource column source containing time information.
     */
    public void registerTimeSource(RowSet rowSet, ColumnSource<Instant> timestampSource) {
        // Does nothing
    }

    /**
     * Refresh the simulated live tables.
     */
    @Override
    public void run() {
        for (PeriodicTask timerTask : timerTasks) {
            Clock clock = clock();
            timerTask.next(clock.instantNanos());
        }

        if (lastLap) {
            shutdown();
        }
        Clock clock = clock();
        if (clock.instantNanos().compareTo(endTime) >= 0) {
            lastLap = true;
        }
    }

    private class SourceRefresher extends InstrumentedUpdateSource {
        SourceRefresher() {
            super(Replayer.this.updateGraph, "Replayer");
        }

        @Override
        protected void instrumentedRefresh() {
            Replayer.this.run();
        }

        @Override
        protected void onRefreshError(Exception error) {
            log.error().append("Unexpected Error Refreshing Replayer: ").append(error).endl();
            shutdown();
        }
    }

    private static class PeriodicTask {

        private final TimerTask task;
        private final long delayMillis;
        private final long periodMillis;
        Instant nextTime = null;

        public PeriodicTask(TimerTask task, long delayMillis, long periodMillis) {
            this.task = task;
            this.delayMillis = delayMillis;
            this.periodMillis = periodMillis;
        }

        public void next(Instant currentTime) {
            if (nextTime == null) {
                nextTime = DateTimeUtils.plus(currentTime, DateTimeUtils.millisToNanos(delayMillis));
            } else {
                if (DateTimeUtils.epochNanos(nextTime) < DateTimeUtils.epochNanos(currentTime)) {
                    try {
                        task.run();
                        nextTime = DateTimeUtils.plus(currentTime, DateTimeUtils.millisToNanos(periodMillis));
                    } catch (Error e) {
                        log.error(e).append("Error").endl();
                    }
                }
            }
        }
    }

    private final CopyOnWriteArrayList<PeriodicTask> timerTasks = new CopyOnWriteArrayList<>();

    /**
     * Gets a handle to the replayer.
     *
     * @return handle to the replayer.
     */
    @Override
    public ReplayerHandle getHandle() {
        return handle;
    }

    @Override
    public Clock clock() {
        return new ClockImpl();
    }

    private class ClockImpl implements Clock {
        @Override
        public long currentTimeMillis() {
            return DateTimeUtils.nanosToMillis(currentTimeNanos());
        }

        @Override
        public long currentTimeMicros() {
            return DateTimeUtils.nanosToMicros(currentTimeNanos());
        }

        /**
         * Simulated time in nanoseconds.
         *
         * @return simulated time in nanoseconds.
         */
        @Override
        public long currentTimeNanos() {
            if (deltaNanos == Long.MAX_VALUE) {
                return DateTimeUtils.epochNanos(startTime);
            }
            final long resultNanos = DateTimeUtils.millisToNanos(System.currentTimeMillis()) - deltaNanos;
            return Math.min(resultNanos, DateTimeUtils.epochNanos(endTime));
        }

        @Override
        public Instant instantNanos() {
            if (deltaNanos == Long.MAX_VALUE) {
                return startTime;
            }
            final long resultNanos = DateTimeUtils.millisToNanos(System.currentTimeMillis()) - deltaNanos;
            if (resultNanos >= DateTimeUtils.epochNanos(endTime)) {
                return endTime;
            }
            return Instant.ofEpochSecond(0, resultNanos);
        }

        @Override
        public Instant instantMillis() {
            return instantNanos();
        }
    }
}
