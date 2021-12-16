/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.replay;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.ShiftObliviousInstrumentedListener;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.time.TimeProvider;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static io.deephaven.time.DateTimeUtils.millisToNanos;
import static io.deephaven.time.DateTimeUtils.nanosToTime;

/**
 * Replay historical data as simulated real-time data.
 */
public class Replayer implements ReplayerInterface, Runnable {
    private static final Logger log = LoggerFactory.getLogger(ShiftObliviousInstrumentedListener.class);

    protected DateTime startTime;
    protected DateTime endTime;
    private long delta = Long.MAX_VALUE;
    private CopyOnWriteArrayList<Runnable> currentTables = new CopyOnWriteArrayList<>();
    private volatile boolean done;
    private boolean lastLap;
    private final ReplayerHandle handle = new ReplayerHandle() {
        @Override
        public Replayer getReplayer() {
            return Replayer.this;
        }
    };

    // Condition variable for use with UpdateGraphProcessor lock - the object monitor is no longer used
    private final Condition ugpCondition = UpdateGraphProcessor.DEFAULT.exclusiveLock().newCondition();

    /**
     * Creates a new replayer.
     *
     * @param startTime start time
     * @param endTime end time
     */
    public Replayer(DateTime startTime, DateTime endTime) {
        this.endTime = endTime;
        this.startTime = startTime;
        currentTables.add(this);
    }

    /**
     * Starts replaying data.
     */
    @Override
    public void start() {
        delta = nanosToTime(millisToNanos(System.currentTimeMillis())).getNanos() - startTime.getNanos();
        for (Runnable currentTable : currentTables) {
            UpdateGraphProcessor.DEFAULT.addSource(currentTable);
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
     *
     * @throws IOException problem shutting down the replayer.
     */
    @Override
    public void shutdown() throws IOException {
        endTime = currentTime();
        if (done) {
            return;
        }
        UpdateGraphProcessor.DEFAULT.removeSources(currentTables);
        currentTables = null;
        if (UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread()) {
            shutdownInternal();
        } else if (UpdateGraphProcessor.DEFAULT.isRefreshThread()) {
            UpdateGraphProcessor.DEFAULT.addNotification(new TerminalNotification() {
                @Override
                public boolean mustExecuteWithUgpLock() {
                    return true;
                }

                @Override
                public void run() {
                    shutdownInternal();
                }
            });
        } else {
            UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(this::shutdownInternal);
        }
    }

    private void shutdownInternal() {
        Assert.assertion(UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread(),
                "UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread()");
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
        UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(() -> {
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
     * @param delay delay in milliseconds before first executing the task
     * @param period frequency in milliseconds to execute the task.
     */
    @Override
    public void schedule(TimerTask task, long delay, long period) {
        timerTasks.add(new PeriodicTask(task, delay, period));
    }

    /**
     * Gets a time provider for the replayer. The time provider returns the current replay time.
     *
     * @param replayer replayer
     * @return time provider that returns the current replay time.
     */
    public static TimeProvider getTimeProvider(final ReplayerInterface replayer) {
        return replayer == null ? new TimeProvider() {
            @Override
            public DateTime currentTime() {
                return DateTimeUtils.currentTime();
            }
        } : new TimeProvider() {
            @Override
            public DateTime currentTime() {
                try {
                    return replayer.currentTime();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * Simulated time in nanoseconds.
     *
     * @return simulated time in nanoseconds.
     */
    public long currentTimeNanos() {
        return currentTime().getNanos();
    }

    /**
     * Simulated time.
     *
     * @return simulated time.
     */
    @Override
    public DateTime currentTime() {
        if (delta == Long.MAX_VALUE)
            return startTime;
        final DateTime result = DateTimeUtils.minus(nanosToTime(millisToNanos(System.currentTimeMillis())), delta);
        if (result.getNanos() > endTime.getNanos()) {
            return endTime;
        }
        return result;
    }

    /**
     * Sets the current replay time.
     *
     * @param updatedTime new replay time.
     */
    @Override
    public void setTime(long updatedTime) {
        if (delta == Long.MAX_VALUE) {
            startTime = DateTimeUtils.millisToTime(updatedTime);
        } else {
            long adjustment = updatedTime - currentTime().getMillis();
            if (adjustment > 0) {
                delta = delta - adjustment * 1000000;
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
        final ReplayTable result =
                new ReplayTable(dataSource.getRowSet(), dataSource.getColumnSourceMap(), timeColumn, this);
        currentTables.add(result);
        if (delta < Long.MAX_VALUE) {
            UpdateGraphProcessor.DEFAULT.addSource(result);
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
        final ReplayGroupedFullTable result = new ReplayGroupedFullTable(dataSource.getRowSet(),
                dataSource.getColumnSourceMap(), timeColumn, this, groupingColumn);
        currentTables.add(result);
        if (delta < Long.MAX_VALUE) {
            UpdateGraphProcessor.DEFAULT.addSource(result);
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
        final ReplayLastByGroupedTable result = new ReplayLastByGroupedTable(dataSource.getRowSet(),
                dataSource.getColumnSourceMap(), timeColumn, this, groupingColumns);
        currentTables.add(result);
        if (delta < Long.MAX_VALUE) {
            UpdateGraphProcessor.DEFAULT.addSource(result);
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
    public void registerTimeSource(RowSet rowSet, ColumnSource<DateTime> timestampSource) {
        // Does nothing
    }

    /**
     * Refresh the simulated live tables.
     */
    @Override
    public void run() {
        for (PeriodicTask timerTask : timerTasks) {
            timerTask.next(currentTime());
        }

        if (lastLap) {
            try {
                shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (currentTime().compareTo(endTime) >= 0) {
            lastLap = true;
        }
    }

    private static class PeriodicTask {

        private final TimerTask task;
        private final long delay;
        private final long period;
        DateTime nextTime = null;

        public PeriodicTask(TimerTask task, long delay, long period) {
            this.task = task;
            this.delay = delay;
            this.period = period;
        }

        public void next(DateTime currentTime) {
            if (nextTime == null) {
                nextTime = DateTimeUtils.plus(currentTime, delay * 1000000);
            } else {
                if (nextTime.getNanos() < currentTime.getNanos()) {
                    try {
                        task.run();
                        nextTime = DateTimeUtils.plus(currentTime, period * 1000000);
                    } catch (Error e) {
                        log.error(e).append("Error").endl();
                    }
                }
            }
        }
    }

    private CopyOnWriteArrayList<PeriodicTask> timerTasks = new CopyOnWriteArrayList<>();

    /**
     * Gets a handle to the replayer.
     *
     * @return handle to the replayer.
     */
    @Override
    public ReplayerHandle getHandle() {
        return handle;
    }
}
