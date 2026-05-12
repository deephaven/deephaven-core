//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.process;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.util.thread.ThreadDump;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is a helper class for keeping track of one-time shutdown tasks. Tasks are dispatched serially according to their
 * ordering category (first, middle, last), and in LIFO (last in, first out) order within their category.
 */
@SuppressWarnings("WeakerAccess")
public class ShutdownManagerImpl implements ShutdownManager {

    private static final Logger log = LoggerFactory.getLogger(ShutdownManagerImpl.class);

    /**
     * Property for configuring "if all else fails" process halt, to prevent zombie processes.
     */
    private static final String SHUTDOWN_TIMEOUT_MILLIS_PROP = "ShutdownManager.shutdownTimeoutMillis";

    /**
     * Timeout for "if all else fails" process halt, to prevent zombie processes.
     */
    private final long SHUTDOWN_TIMEOUT_MILLIS =
            Configuration.getInstance().getLongWithDefault(SHUTDOWN_TIMEOUT_MILLIS_PROP, -1);

    /**
     * Shutdown task stacks by ordering category. Note, EnumMaps iterate in ordinal order.
     */
    private final Map<OrderingCategory, SynchronizedStack<Task>> tasksByOrderingCategory;
    {
        final EnumMap<OrderingCategory, SynchronizedStack<Task>> taskStacksByOrderingCategoryTemp =
                new EnumMap<>(OrderingCategory.class);
        Arrays.stream(OrderingCategory.values())
                .forEach(oc -> taskStacksByOrderingCategoryTemp.put(oc, new SynchronizedStack<>()));
        tasksByOrderingCategory = Collections.unmodifiableMap(taskStacksByOrderingCategoryTemp);
    }

    private enum State {
        NORMAL(false), SHUTDOWN_INVOKED(true), FINISHED(true);

        private final boolean tasksInvoked;

        State(boolean tasksInvoked) {
            this.tasksInvoked = tasksInvoked;
        }
    }

    private final Lock stateLock = new ReentrantLock();
    private final Condition finished = stateLock.newCondition();
    /**
     * Allow the implementation to ensure that shutdown processing is only invoked once.
     */
    private volatile State state = State.NORMAL;
    private static final AtomicReferenceFieldUpdater<ShutdownManagerImpl, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ShutdownManagerImpl.class, State.class, "state");

    /**
     * Construct a new ShutdownManager.
     */
    public ShutdownManagerImpl() {}

    @Override
    public void addShutdownHookToRuntime() {
        Runtime.getRuntime().addShutdownHook(newShutdownHookThread());
    }

    @VisibleForTesting
    Thread newShutdownHookThread() {
        return new Thread(() -> {
            final boolean didInvokeTasks = maybeInvokeTasks();
            // If some other thread has already invoked maybeInvokeTasks, we still want to block shutdown on completion
            // of those tasks.
            if (!didInvokeTasks) {
                try {
                    awaitTasksFinished();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    @Override
    public void registerTask(@NotNull final OrderingCategory orderingCategory, @NotNull final Task task) {
        tasksByOrderingCategory.get(Require.neqNull(orderingCategory, "orderingCategory")).push(task);
    }

    @Override
    public void deregisterTask(@NotNull final OrderingCategory orderingCategory, @NotNull final Task task) {
        tasksByOrderingCategory.get(Require.neqNull(orderingCategory, "orderingCategory")).remove(task);
    }

    @Override
    public void reset() {
        tasksByOrderingCategory.values().forEach(SynchronizedStack::clear);
        do {
            switch (state) {
                case NORMAL:
                    return;
                case SHUTDOWN_INVOKED:
                    throw new IllegalStateException("Unsafe to reset while shutdown tasks are being invoked");
            }
        } while (!STATE_UPDATER.compareAndSet(this, State.FINISHED, State.NORMAL));
    }

    @Override
    public boolean tasksInvoked() {
        return state.tasksInvoked;
    }

    @Override
    public boolean maybeInvokeTasks() {
        if (!STATE_UPDATER.compareAndSet(this, State.NORMAL, State.SHUTDOWN_INVOKED)) {
            return false;
        }
        try {
            logShutdown(LogLevel.WARN, "Initiating shutdown processing");
            installTerminator();
            tasksByOrderingCategory.forEach((oc, tasks) -> {
                logShutdown(LogLevel.WARN, "Starting to invoke ", oc, " shutdown tasks");
                Task task;
                while ((task = tasks.pop()) != null) {
                    try {
                        task.invoke();
                    } catch (Throwable t) {
                        logShutdown(LogLevel.ERROR, "Shutdown task ", task, " threw ", t);
                    }
                }
                logShutdown(LogLevel.WARN, "Done invoking ", oc, " shutdown tasks");
            });
            logShutdown(LogLevel.WARN, "Finished shutdown processing");
            return true;
        } finally {
            // Normally, this setter would be in guarded in the lock. In our case, we can do without since it's volatile
            // and we are guaranteeing that the state can't be modified anywhere else while the state is
            // SHUTDOWN_INVOKED (for example, in reset).
            state = State.FINISHED;
            stateLock.lock();
            try {
                finished.signalAll();
            } finally {
                stateLock.unlock();
            }
        }
    }

    @Override
    public void awaitTasksFinished() throws InterruptedException {
        if (state == State.FINISHED) {
            return;
        }
        stateLock.lock();
        try {
            while (state != State.FINISHED) {
                finished.await();
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Simple synchronized wrapper around some Deque methods.
     *
     * @param <T>
     */
    private static class SynchronizedStack<T> {

        private final Deque<T> storage = new ArrayDeque<>();

        public synchronized void push(@NotNull final T value) {
            storage.offerLast(Require.neqNull(value, "value"));
        }

        public synchronized @Nullable T pop() {
            return storage.pollLast();
        }

        public synchronized void remove(@NotNull final T value) {
            storage.removeLastOccurrence(Require.neqNull(value, "value"));
        }

        public synchronized void clear() {
            storage.clear();
        }
    }

    /**
     * Attempt to log a line of items. Fails silently if any Throwable is thrown, including Throwables one might
     * ordinarily prefer not to catch (e.g. InterruptedException, subclasses of Error, etc). This is intended for use in
     * processes that are shutting down.
     */
    public static void logShutdown(final LogLevel level, final Object... items) {
        try {
            LogEntry entry = log.getEntry(level);
            for (Object item : items) {
                entry.append(item.toString());
            }
            entry.endl();
        } catch (Throwable ignored) {
        }
    }

    /**
     * Watchdog thread that will halt the application if it fails to finish in the configured amount of time.
     */
    private void ensureTermination() {
        final long start = System.nanoTime();
        final long deadline = start + TimeUnit.MILLISECONDS.toNanos(SHUTDOWN_TIMEOUT_MILLIS);
        for (long now = start; now < deadline; now = System.nanoTime()) {
            final long nanosRemaining = deadline - now;
            final long millisRemainingRoundedUp =
                    TimeUnit.NANOSECONDS.toMillis(nanosRemaining + TimeUnit.MILLISECONDS.toNanos(1) - 1);
            try {
                Thread.sleep(millisRemainingRoundedUp);
            } catch (InterruptedException ignored) {
            }
        }

        final PrintStream destStdErr = PrintStreamGlobals.getErr();
        destStdErr
                .println("Halting due to shutdown delay greater than " + SHUTDOWN_TIMEOUT_MILLIS + "ms. Thread dump:");
        try {
            ThreadDump.threadDump(destStdErr);
            destStdErr.println();
            destStdErr.println("Halted due to shutdown delay greater than " + SHUTDOWN_TIMEOUT_MILLIS + "ms");
        } catch (Throwable t) {
            destStdErr.println("Failed to generate thread dump: " + t);
        } finally {
            destStdErr.flush();
            Runtime.getRuntime().halt(17);
        }
    }

    /**
     * If configured to do so, start a watchdog thread that will halt the application if it gets hung during shutdown.
     */
    private void installTerminator() {
        if (SHUTDOWN_TIMEOUT_MILLIS >= 0) {
            final Thread terminator = new Thread(this::ensureTermination, "ShutdownTimeoutTerminator");
            terminator.setDaemon(true);
            terminator.start();
        }
    }
}
