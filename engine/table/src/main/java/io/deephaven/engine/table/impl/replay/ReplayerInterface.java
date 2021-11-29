/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.replay;

import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTime;

import java.io.IOException;
import java.util.TimerTask;

/**
 * An interface for replaying historical data as simulated real-time data.
 */
public interface ReplayerInterface {
    /**
     * Starts replaying data.
     */
    void start() throws IOException;

    /**
     * Has the replayer finished replaying all data.
     *
     * @return true if the replayer has finished replaying all data; false otherwise.
     */
    boolean isDone() throws IOException;

    /**
     * Shuts down the replayer.
     *
     * @throws IOException problem shutting down the replayer.
     */
    void shutdown() throws IOException;

    /**
     * Wait a specified interval for the replayer to complete.  If the replayer has not completed by the
     * end of the interval, the method returns.
     *
     * @param maxTimeMillis maximum number of milliseconds to wait.
     * @throws IOException problems encountered
     */
    void waitDone(long maxTimeMillis) throws IOException;


    /**
     * Sets the current replay time.
     *
     * @param updatedTime new replay time.
     */
    void setTime(long updatedTime);

    /**
     * Schedule a task to execute.
     *
     * @param task task to execute
     * @param delay delay in milliseconds before first executing the task
     * @param period frequency in milliseconds to execute the task.
     */
    void schedule(TimerTask task, long delay, long period);

    /**
     * Simulated time.
     *
     * @return simulated time.
     */
    DateTime currentTime() throws IOException;

    /**
     * Prepares a historical table for replaying.
     *
     * @param dataSource historical table to replay
     * @param timeColumn column in the table containing timestamps
     * @return dynamic, replayed version of the table.
     */
    Table replay(Table dataSource,String timeColumn) throws IOException;

    /**
     * Prepares a grouped historical table for replaying.  This method can be faster than the ungrouped replay, but
     * the performance increase comes with a cost.  Within a group, the data ordering is maintained.  Between groups,
     * data ordering is not maintained for a time interval.
     *
     * @param dataSource historical table to replay
     * @param timeColumn column in the table containing timestamps
     * @return dynamic, replayed version of the table.
     */
    Table replayGrouped(Table dataSource, String timeColumn, String byColumn) throws IOException;

    /**
     * Prepares a grouped historical table for replaying as a last-by table.
     *
     * @param dataSource historical table to replay
     * @param timeColumn column in the table containing timestamps
     * @param groupingColumns columns used as the key in computing last-by
     * @return dynamic, replayed version of the last-by table.
     */
    Table replayGroupedLastBy(Table dataSource, String timeColumn, String... groupingColumns) throws IOException;

    /**
     * Gets a handle to the replayer.
     *
     * @return handle to the replayer.
     */
    ReplayerHandle getHandle();
}
