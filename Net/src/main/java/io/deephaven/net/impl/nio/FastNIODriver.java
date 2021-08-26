/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.net.impl.nio;

import io.deephaven.base.Procedure;
import io.deephaven.base.UnfairMutex;
import io.deephaven.configuration.Configuration;
import io.deephaven.net.CommBase;
import io.deephaven.io.NioUtil;
import io.deephaven.io.logger.LogCrashDump;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.Scheduler;
import io.deephaven.io.sched.TimedJob;
import io.deephaven.io.sched.YASchedulerImpl;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class FastNIODriver implements Runnable {
    private static Logger log;

    public static int numTotalThreads(String property) {
        final String[] values = Configuration.getInstance().getProperty(property).split(",");
        return Integer.parseInt(values[0]) * Integer.parseInt(values[1]);
    }

    public static int threadsPerScheduler(String property) {
        final String[] values = Configuration.getInstance().getProperty(property).split(",");
        if (values.length != 6)
            return 0;
        return Integer.parseInt(values[1]);
    }

    public static Scheduler[] createSchedulers(String name, String property, Logger log) {
        return createSchedulers(name, property, log, Configuration.getInstance());
    }

    public static Scheduler[] createSchedulers(String name, String property, Logger log, Configuration config) {
        final String[] values = config.getProperty(property).split(",");
        if (values.length != 6)
            return null;

        final int numSchedulers = Integer.parseInt(values[0]);
        final int threadsPerScheduler = Integer.parseInt(values[1]);
        final long timeoutsOrSpins = Long.parseLong(values[2]);
        final int spinsUntilPark = Integer.parseInt(values[3]);
        final boolean doTimingStats = Boolean.parseBoolean(values[4]);
        final boolean doSpinSelect = Boolean.parseBoolean(values[5]);
        final Scheduler[] schedulers = new Scheduler[numSchedulers];
        for (int i = 0; i < numSchedulers; ++i) {
            schedulers[i] = createDrivers(name + "-" + i, log, threadsPerScheduler, threadsPerScheduler,
                    timeoutsOrSpins, spinsUntilPark, false, doTimingStats, doSpinSelect).getScheduler();
        }
        return schedulers;
    }

    public static FastNIODriver createDrivers(String name, Logger log, int initialThreads, int maxThreads,
            long workTimeout, int spinsUntilPark, boolean crashOnMax) {
        return createDrivers(name, log, initialThreads, maxThreads, workTimeout, spinsUntilPark, crashOnMax, true,
                false);
    }

    public static FastNIODriver createDrivers(String name, Logger log, int initialThreads, int maxThreads,
            long workTimeout, int spinsUntilPark, boolean crashOnMax, boolean doTimingStats, boolean doSpinSelect) {
        FastNIODriver.log = log;
        log.info().append(name).append(": Starting FastNIODriver Scheduler: threads: ").append(initialThreads)
                .append(", maxThreads: ").append(maxThreads)
                .append(", workTimeout/spinsOnSelect: ").append(workTimeout)
                .append(", spinsUntilPark: ").append(spinsUntilPark)
                .append(", doSpinSelect: ").append(doSpinSelect)
                .endl();
        try {
            final Scheduler scheduler = new YASchedulerImpl(name, NioUtil.reduceSelectorGarbage(Selector.open()), log,
                    doTimingStats, doSpinSelect);

            final UnfairMutex mutex = new UnfairMutex(spinsUntilPark, maxThreads);
            final AtomicBoolean shutdown = new AtomicBoolean(false);
            final AtomicInteger created = new AtomicInteger(0);
            final AtomicInteger destroyed = new AtomicInteger(0);
            final AtomicInteger available = new AtomicInteger(0);
            final InternalThread[] threads = new InternalThread[initialThreads];
            // separate the creation and start so the created / available values are setup
            for (int i = 0; i < initialThreads; ++i) {
                threads[i] = createNewThread(name, scheduler, mutex, shutdown, workTimeout, created, destroyed,
                        available, maxThreads, crashOnMax);
            }
            for (int i = 0; i < initialThreads; ++i) {
                threads[i].start();
            }

            return threads[0].driver;
        } catch (IOException x) {
            CommBase.signalFatalError(name + ": FastNIODriver can't create scheduler", x);
            return null;
        }
    }

    private static class InternalThread extends Thread {
        private final FastNIODriver driver;

        private InternalThread(final FastNIODriver driver) {
            super(driver);
            this.driver = driver;
        }
    }

    private static InternalThread createNewThread(final String name, final Scheduler scheduler, final UnfairMutex mutex,
            final AtomicBoolean shutdown, final long workTimeout, final AtomicInteger created,
            final AtomicInteger destroyed, final AtomicInteger available, final int maxThreads,
            final boolean crashOnMax) {
        InternalThread t = new InternalThread(new FastNIODriver(name, scheduler, mutex, shutdown, workTimeout, created,
                destroyed, available, maxThreads, crashOnMax));
        t.setDaemon(true);
        t.setName(name + "-FastNIODriver-" + created.getAndIncrement());
        int a = available.incrementAndGet();
        log.info().append("Creating thread ").append(t.getName()).append(". available: ").append(a).endl();
        return t;
    }

    private final Scheduler scheduler;
    private final UnfairMutex mutex;
    private final AtomicBoolean shutdown;
    private final long workTimeout;
    private final Procedure.Nullary mutexUnlockHandoff;
    private boolean alreadyHandedOff;

    private final AtomicInteger created;
    private final AtomicInteger destroyed;
    private final AtomicInteger available;
    private final int maxThreads;
    private final boolean crashOnMax;

    private FastNIODriver(final String name, final Scheduler scheduler, final UnfairMutex mutex,
            final AtomicBoolean shutdown, final long workTimeout, final AtomicInteger created,
            final AtomicInteger destroyed, final AtomicInteger available, final int maxThreads,
            final boolean crashOnMax) {
        this.scheduler = scheduler;
        this.mutex = mutex;
        this.shutdown = shutdown;
        this.workTimeout = workTimeout;
        this.created = created;
        this.destroyed = destroyed;
        this.available = available;
        this.maxThreads = maxThreads;
        this.crashOnMax = crashOnMax;
        alreadyHandedOff = false;
        mutexUnlockHandoff = new Procedure.Nullary() {
            @Override
            public void call() {
                if (!alreadyHandedOff) {
                    if (shouldCreate()) {
                        // nobody to handoff too! let's create a new driver
                        createNewThread(name, scheduler, mutex, shutdown, workTimeout, created, destroyed, available,
                                maxThreads, crashOnMax).start();
                    }
                    mutex.unlock();
                    alreadyHandedOff = true;
                }
            }
        };
    }

    // only called when we have the mutex...
    private boolean shouldCreate() {
        if (available.get() == 0) {
            // don't need to worry about races w/ index b/c we have lock
            if (created.get() == maxThreads) {
                if (crashOnMax) {
                    log.fatal().append("FastNIODriver: exceeded maximum thread pool limit: ").append(summary()).endl();
                    LogCrashDump.logCrashDump(log);
                    CommBase.signalFatalError("FastNIODriver: exceeded maximum thread pool limit: " + summary(),
                            new Throwable());
                }
                return false;
            }
            return true;
        }
        return false;
    }

    public String summary() {
        return "(available: " + available.get() + ", created: " + created.get() + ", destroyed: " + destroyed.get()
                + ")";
    }

    @Override
    public void run() {
        final Thread me = Thread.currentThread();
        Throwable throwable = null;
        while (true) {
            if (shutdown.get()) {
                break;
            }
            mutex.lock();
            alreadyHandedOff = false;
            if (shutdown.get()) {
                mutexUnlockHandoff.call();
                break;
            }

            try {

                available.getAndDecrement();
                do {
                    scheduler.work(workTimeout, mutexUnlockHandoff);
                } while (mutex.getOwner() == me);
                available.getAndIncrement();

            } catch (Throwable x) {
                throwable = x;
                shutdown.set(true);
                scheduler.installJob(new TimedJob() {
                    public void timedOut() {}
                }, 0); // wake us up yo
                mutexUnlockHandoff.call(); // we aren't sure whether the scheduler.work has already called the handoff
                                           // or not yet, so go ahead and call it (it won't double release it)
                long deadline = System.currentTimeMillis() + 5000;
                // b/c we haven't destroyed ourself yet...
                // meh spinning :/
                while (created.get() != destroyed.get() + 1) {
                    if (deadline - System.currentTimeMillis() < 0) {
                        break;
                    }
                    Thread.yield(); // better than spinning?
                }

                break;
            }
        }

        if (destroyed.incrementAndGet() == created.get()) {
            scheduler.close();
        }

        if (throwable == null) {
            log.error().append("Thread ").append(me.getName()).append(" is terminating: ").append(summary()).endl();
        } else {
            log.fatal(throwable).append("Thread ").append(me.getName()).append(" is terminating on a fatal exception: ")
                    .append(summary()).endl();
        }

        if (throwable != null)
            CommBase.signalFatalError("Unhandled throwable from FastNIODriver scheduler", throwable);
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    public boolean shutdown(long maxWait) {
        shutdown.set(true);
        scheduler.installJob(new TimedJob() {
            public void timedOut() {}
        }, 0);
        long deadline = System.currentTimeMillis() + maxWait;
        while (created.get() != destroyed.get()) {
            if (deadline - System.currentTimeMillis() < 0) {
                break;
            }
            try {
                Thread.sleep(1); // better than spinning?
            } catch (InterruptedException e) {
                // ignore
            }
        }

        return created.get() == destroyed.get();
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    // whitebox test support methods
    public int junit_getWaiting() {
        return available.get();
    }

    public int junit_getCreated() {
        return created.get();
    }

    public int junit_getDestroyed() {
        return destroyed.get();
    }
}
