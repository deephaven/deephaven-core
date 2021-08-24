/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.replay;

import io.deephaven.db.exceptions.QueryCancellationException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.InstrumentedListener;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.TimeProvider;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static io.deephaven.db.tables.utils.DBTimeUtils.millisToNanos;
import static io.deephaven.db.tables.utils.DBTimeUtils.nanosToTime;

/**
 * Replay historical data as simulated real-time data.
 */
public class Replayer implements ReplayerInterface, LiveTable {
    private static final Logger log = LoggerFactory.getLogger(InstrumentedListener.class);

    protected DBDateTime startTime;
    protected DBDateTime endTime;
    private long delta = Long.MAX_VALUE;
    private CopyOnWriteArrayList<LiveTable> currentTables = new CopyOnWriteArrayList<>();
    private boolean done;
    private boolean lastLap;
    private final ReplayerHandle handle = new ReplayerHandle() {
        @Override
        public Replayer getReplayer() {
            return Replayer.this;
        }
    };

    // Condition variable for use with LiveTableMonitor lock - the object monitor is no longer used
    private final Condition ltmCondition = LiveTableMonitor.DEFAULT.exclusiveLock().newCondition();

    /**
     * Creates a new replayer.
     *
     * @param startTime start time
     * @param endTime end time
     */
    public Replayer(DBDateTime startTime, DBDateTime endTime) {
        this.endTime = endTime;
        this.startTime = startTime;
        currentTables.add(this);
    }

    /**
     * Starts replaying data.
     */
    @Override
    public void start() {
        delta = nanosToTime(millisToNanos(System.currentTimeMillis())).getNanos()
            - startTime.getNanos();
        for (LiveTable currentTable : currentTables) {
            LiveTableMonitor.DEFAULT.addTable(currentTable);
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
        LiveTableMonitor.DEFAULT.removeTables(currentTables);
        currentTables = null;
        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(() -> {
            done = true;
            ltmCondition.signalAll();
        });
    }

    /**
     * Wait a specified interval for the replayer to complete. If the replayer has not completed by
     * the end of the interval, the method returns.
     *
     * @param maxTimeMillis maximum number of milliseconds to wait.
     * @throws QueryCancellationException thread was interrupted.
     */
    @Override
    public void waitDone(long maxTimeMillis) {
        long expiryTime = System.currentTimeMillis() + maxTimeMillis;
        if (done) {
            return;
        }
        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(() -> {
            while (!done && expiryTime > System.currentTimeMillis()) {
                try {
                    ltmCondition.await(expiryTime - System.currentTimeMillis(),
                        TimeUnit.MILLISECONDS);
                } catch (InterruptedException interruptIsCancel) {
                    throw new QueryCancellationException("Interrupt detected", interruptIsCancel);
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
            public DBDateTime currentTime() {
                return DBTimeUtils.currentTime();
            }
        } : new TimeProvider() {
            @Override
            public DBDateTime currentTime() {
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
    public DBDateTime currentTime() {
        if (delta == Long.MAX_VALUE)
            return startTime;
        final DBDateTime result =
            DBTimeUtils.minus(nanosToTime(millisToNanos(System.currentTimeMillis())), delta);
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
            startTime = DBTimeUtils.millisToTime(updatedTime);
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
    public DynamicTable replay(Table dataSource, String timeColumn) {
        final ReplayTable result = new ReplayTable(dataSource.getIndex(),
            dataSource.getColumnSourceMap(), timeColumn, this);
        currentTables.add(result);
        if (delta < Long.MAX_VALUE) {
            LiveTableMonitor.DEFAULT.addTable(result);
        }
        return result;
    }

    /**
     * Prepares a grouped historical table for replaying. This method can be faster than the
     * ungrouped replay, but the performance increase comes with a cost. Within a group, the data
     * ordering is maintained. Between groups, data ordering is not maintained for a time interval.
     *
     * @param dataSource historical table to replay
     * @param timeColumn column in the table containing timestamps
     * @return dynamic, replayed version of the table.
     */
    @Override
    public DynamicTable replayGrouped(Table dataSource, String timeColumn, String groupingColumn) {
        final ReplayGroupedFullTable result = new ReplayGroupedFullTable(dataSource.getIndex(),
            dataSource.getColumnSourceMap(), timeColumn, this, groupingColumn);
        currentTables.add(result);
        if (delta < Long.MAX_VALUE) {
            LiveTableMonitor.DEFAULT.addTable(result);
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
    public DynamicTable replayGroupedLastBy(Table dataSource, String timeColumn,
        String... groupingColumns) {
        final ReplayLastByGroupedTable result = new ReplayLastByGroupedTable(dataSource.getIndex(),
            dataSource.getColumnSourceMap(), timeColumn, this, groupingColumns);
        currentTables.add(result);
        if (delta < Long.MAX_VALUE) {
            LiveTableMonitor.DEFAULT.addTable(result);
        }
        return result;
    }

    /**
     * Register the time column and index from a new table to replay. Most users will use
     * <code>replay</code>, <code>replayGrouped</code>, or <code>replayGroupedLastBy</code> instead
     * of this function.
     *
     * @param index table index
     * @param timestampSource column source containing time information.
     */
    public void registerTimeSource(Index index, ColumnSource<DBDateTime> timestampSource) {
        // Does nothing
    }

    /**
     * Refresh the simulated live tables.
     */
    @Override
    public void refresh() {
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
        DBDateTime nextTime = null;

        public PeriodicTask(TimerTask task, long delay, long period) {
            this.task = task;
            this.delay = delay;
            this.period = period;
        }

        public void next(DBDateTime currentTime) {
            if (nextTime == null) {
                nextTime = DBTimeUtils.plus(currentTime, delay * 1000000);
            } else {
                if (nextTime.getNanos() < currentTime.getNanos()) {
                    try {
                        task.run();
                        nextTime = DBTimeUtils.plus(currentTime, period * 1000000);
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
