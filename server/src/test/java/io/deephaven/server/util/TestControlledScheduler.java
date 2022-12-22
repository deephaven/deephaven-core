/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.util;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import io.deephaven.base.clock.ClockNanoBase;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;

public class TestControlledScheduler extends ClockNanoBase implements Scheduler {

    private static final Logger log = LoggerFactory.getLogger(TestControlledScheduler.class);

    private long currentTimeInNs = 0;

    private final TreeMultimap<DateTime, Runnable> workQueue =
            TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());

    @Override
    public boolean inTestMode() {
        return true;
    }

    /**
     * Runs the first queued command if there are any.
     */
    public void runOne() {
        if (workQueue.isEmpty()) {
            return;
        }

        try {
            final Map.Entry<DateTime, Collection<Runnable>> entry = workQueue.asMap().firstEntry();
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
     * Will run commands until all work items that should be run before Max(currentTime, untilTime) have run. Does not
     * execute events scheduled at the provided time.
     *
     * @param untilTime time to run until
     */
    public void runUntil(final DateTime untilTime) {
        while (!workQueue.isEmpty()) {
            final long now = Math.max(currentTimeInNs, untilTime.getNanos());
            if (workQueue.asMap().firstEntry().getKey().getNanos() >= now) {
                break;
            }

            runOne();
        }

        currentTimeInNs = Math.max(currentTimeInNs, untilTime.getNanos());
    }

    public void runThrough(final long throughTimeMillis) {
        runThroughNanos(throughTimeMillis * 1_000_000);
    }

    /**
     * Will run commands until all work items that should be run through Max(currentTime, untilTime) have run. Does
     * execute events scheduled at the provided time.
     *
     * @param throughTimeNanos time to run through
     */
    public void runThroughNanos(final long throughTimeNanos) {
        while (!workQueue.isEmpty()) {
            final long now = Math.max(currentTimeInNs, throughTimeNanos);
            if (workQueue.asMap().firstEntry().getKey().getNanos() > now) {
                break;
            }

            runOne();
        }

        currentTimeInNs = Math.max(currentTimeInNs, throughTimeNanos);
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
     * Helper to give you a DateTime after a certain time has passed on the simulated clock.
     *
     * @param delayInMs the number of milliseconds to add to current time
     * @return a DateTime representing {@code now + delayInMs}
     */
    public DateTime timeAfterMs(final long delayInMs) {
        return DateTimeUtils.nanosToTime(currentTimeInNs + DateTimeUtils.millisToNanos(delayInMs));
    }

    @Override
    public long currentTimeNanos() {
        return currentTimeInNs;
    }

    @Override
    public void runAtTime(long epochMillis, @NotNull Runnable command) {
        workQueue.put(DateTimeUtils.millisToTime(epochMillis), command);
    }

    @Override
    public void runAfterDelay(final long delayMs, final @NotNull Runnable command) {
        workQueue.put(DateTimeUtils.nanosToTime(currentTimeInNs + delayMs * 1_000_000L), command);
    }

    @Override
    public void runImmediately(final @NotNull Runnable command) {
        workQueue.put(DateTime.of(this), command);
    }

    @Override
    public void runSerially(@NotNull Runnable command) {
        // we aren't multi-threaded anyways
        runImmediately(command);
    }
}
