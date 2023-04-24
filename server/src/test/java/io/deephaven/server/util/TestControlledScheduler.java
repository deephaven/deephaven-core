/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.util;

import io.deephaven.base.Pair;
import io.deephaven.base.clock.ClockNanoBase;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

public class TestControlledScheduler extends ClockNanoBase implements Scheduler {

    private static final Logger log = LoggerFactory.getLogger(TestControlledScheduler.class);

    private volatile long currentTimeInNs = 0;

    private final Queue<Pair<DateTime, Runnable>> workQueue =
            new PriorityBlockingQueue<>(11, Comparator.comparing(Pair::getFirst));

    @Override
    public boolean inTestMode() {
        return true;
    }

    /**
     * Runs the first queued command if there are any.
     */
    public synchronized void runOne() {
        final Pair<DateTime, Runnable> item = workQueue.poll();
        if (item == null) {
            return;
        }

        currentTimeInNs = Math.max(currentTimeInNs, item.getFirst().getNanos());
        try {
            item.getSecond().run();
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
    public synchronized void runUntil(final DateTime untilTime) {
        Pair<DateTime, Runnable> item;
        while ((item = workQueue.peek()) != null) {
            final long now = Math.max(currentTimeInNs, untilTime.getNanos());
            if (item.getFirst().getNanos() >= now) {
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
    public synchronized void runThroughNanos(final long throughTimeNanos) {
        Pair<DateTime, Runnable> item;
        while ((item = workQueue.peek()) != null) {
            final long now = Math.max(currentTimeInNs, throughTimeNanos);
            if (item.getFirst().getNanos() > now) {
                break;
            }

            runOne();
        }

        currentTimeInNs = Math.max(currentTimeInNs, throughTimeNanos);
    }

    /**
     * Will run commands until all work items have been run.
     */
    public synchronized void runUntilQueueEmpty() {
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
        return DateTimeUtils.nanosToDateTime(currentTimeInNs + DateTimeUtils.millisToNanos(delayInMs));
    }

    @Override
    public long currentTimeNanos() {
        return currentTimeInNs;
    }

    @Override
    public void runAtTime(long epochMillis, @NotNull Runnable command) {
        workQueue.add(new Pair<>(DateTimeUtils.millisToDateTime(epochMillis), command));
    }

    @Override
    public void runAfterDelay(final long delayMs, final @NotNull Runnable command) {
        workQueue.add(new Pair<>(DateTimeUtils.nanosToDateTime(currentTimeInNs + delayMs * 1_000_000L), command));
    }

    @Override
    public void runImmediately(final @NotNull Runnable command) {
        workQueue.add(new Pair<>(DateTime.of(this), command));
    }

    @Override
    public void runSerially(@NotNull Runnable command) {
        runImmediately(command);
    }
}
