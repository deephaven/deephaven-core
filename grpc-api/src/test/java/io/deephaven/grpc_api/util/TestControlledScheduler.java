package io.deephaven.grpc_api.util;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;

public class TestControlledScheduler implements Scheduler {

    private static final Logger log = LoggerFactory.getLogger(TestControlledScheduler.class);

    private long currentTimeInNs = 0;

    private final TreeMultimap<DBDateTime, Runnable> workQueue =
        TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());

    /**
     * Runs the first queued command if there are any.
     */
    public void runOne() {
        if (workQueue.isEmpty()) {
            return;
        }

        try {
            final Map.Entry<DBDateTime, Collection<Runnable>> entry =
                workQueue.asMap().firstEntry();
            final Runnable runner = entry.getValue().iterator().next();

            currentTimeInNs = Math.max(currentTimeInNs, entry.getKey().getNanos());
            workQueue.remove(entry.getKey(), runner);

            runner.run();
        } catch (final Exception exception) {
            log.error().append("Exception while running task: ").append(exception).endl();
            throw exception;
        }
    }

    /**
     * Will run commands until all work items that should be run before Max(currentTime, untilTime)
     * have run. Does not execute events scheduled at the provided time.
     *
     * @param untilTime time to run until
     */
    public void runUntil(final DBDateTime untilTime) {
        while (!workQueue.isEmpty()) {
            final long now = Math.max(currentTimeInNs, untilTime.getNanos());
            if (workQueue.asMap().firstEntry().getKey().getNanos() >= now) {
                break;
            }

            runOne();
        }

        currentTimeInNs = Math.max(currentTimeInNs, untilTime.getNanos());
    }

    /**
     * Will run commands until all work items that should be run through Max(currentTime, untilTime)
     * have run. Does execute events scheduled at the provided time.
     *
     * @param throughTime time to run through
     */
    public void runThrough(final DBDateTime throughTime) {
        while (!workQueue.isEmpty()) {
            final long now = Math.max(currentTimeInNs, throughTime.getNanos());
            if (workQueue.asMap().firstEntry().getKey().getNanos() > now) {
                break;
            }

            runOne();
        }

        currentTimeInNs = Math.max(currentTimeInNs, throughTime.getNanos());
    }

    /**
     * Will run commands until all work items have been run.
     */
    public void runUntilQueueEmpty() {
        while (!workQueue.isEmpty()) {
            runOne();
        }
    }

    /**
     * Helper to give you a DBDateTime after a certain time has passed on the simulated clock.
     *
     * @param delayInMs the number of milliseconds to add to current time
     * @return a DBDateTime representing {@code now + delayInMs}
     */
    public DBDateTime timeAfterMs(final long delayInMs) {
        return DBTimeUtils.nanosToTime(currentTimeInNs + DBTimeUtils.millisToNanos(delayInMs));
    }

    @Override
    public DBDateTime currentTime() {
        return DBTimeUtils.nanosToTime(currentTimeInNs);
    }

    @Override
    public void runAtTime(final @NotNull DBDateTime absoluteTime, final @NotNull Runnable command) {
        workQueue.put(absoluteTime, command);
    }

    @Override
    public void runAfterDelay(final long delayMs, final @NotNull Runnable command) {
        workQueue.put(DBTimeUtils.nanosToTime(currentTimeInNs + delayMs * 1_000_000L), command);
    }

    @Override
    public void runImmediately(final @NotNull Runnable command) {
        workQueue.put(currentTime(), command);
    }

    @Override
    public void runSerially(@NotNull Runnable command) {
        // we aren't multi-threaded anyways
        runImmediately(command);
    }
}
