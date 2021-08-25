/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.net.impl.nio;

import io.deephaven.internal.log.LoggerFactory;
import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicInteger;

import io.deephaven.net.CommBase;
import io.deephaven.base.Procedure;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.NioUtil;
import io.deephaven.io.logger.LogCrashDump;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.Scheduler;
import io.deephaven.io.sched.TimedJob;
import io.deephaven.io.sched.YASchedulerImpl;
import io.deephaven.util.process.ProcessEnvironment;

public class NIODriver implements Runnable {
    private static Logger log;

    private static boolean initialized = false;
    private static volatile boolean stopped = false;

    private static Scheduler sched = null;
    private static FastNIODriver driver = null;

    private static final Object lock = new Object();
    private static Thread leader = null;
    private static AtomicInteger available = new AtomicInteger(0);
    private static int created = 0;
    private static int destroyed = 0;

    public static int WORK_TIMEOUT;
    public static int NUM_INITIAL_THREADS;
    public static int HARD_MAX_THREADS;

    private static final boolean useFastNIODriver =
        Configuration.getInstance().getBoolean("NIO.driver.useFast");

    /**
     * Let another thread take over the leadership.
     */
    private static void handoff() {
        Thread me = Thread.currentThread();
        synchronized (lock) {
            if (leader != me) {
                LogCrashDump.logCrashDump(log);
                CommBase.signalFatalError("NIODriver: WTF? in handoff(), but not the leader?",
                    new Throwable());
            }

            if (log.isDebugEnabled()) {
                log.debug().append("Thread ").append(me.getName())
                    .append(" is giving up leadership").endl();
            }

            leader = null;

            if (stopped || available.get() != 0) {
                lock.notify();
            } else {
                // no joy, have to add another thread
                log.warn().append("Thread ").append(me.getName())
                    .append(" is handing off with no threads available: ").append(summary()).endl();
                addThread();
            }
        }
    }

    /**
     * A procedure which calls handoff(), to give the scheduler when we are running full-bore
     */
    private static final Procedure.Nullary handoffProc = new Procedure.Nullary() {
        public void call() {
            handoff();
        }
    };

    /**
     * return a string telling how many threads are doing what
     */
    public static String summary() {
        if (useFastNIODriver) {
            return driver.summary();
        } else {
            return "(available: " + available + ", created: " + created + ", destroyed: "
                + destroyed + ")";
        }
    }

    /**
     * one-time initialization
     */
    public static void init() {
        if (!initialized) {
            final Logger log;
            final ProcessEnvironment processEnvironment = ProcessEnvironment.tryGet();
            if (processEnvironment == null) {
                log = LoggerFactory.getLogger(NIODriver.class);
            } else {
                log = processEnvironment.getLog();
            }
            init(log);
        }
    }

    public static void init(Logger log) {
        synchronized (lock) {
            if (!initialized) {
                NIODriver.log = log;
                WORK_TIMEOUT = Configuration.getInstance().getInteger("NIO.driver.workTimeout");
                NUM_INITIAL_THREADS =
                    Configuration.getInstance().getInteger("NIO.driver.initialThreadCount");
                HARD_MAX_THREADS =
                    Configuration.getInstance().getInteger("NIO.driver.maxThreadCount");
                if (useFastNIODriver) {
                    driver = FastNIODriver.createDrivers("Static", log, NUM_INITIAL_THREADS,
                        HARD_MAX_THREADS, WORK_TIMEOUT, 1000, true);
                    sched = driver.getScheduler();
                } else {
                    try {
                        sched = new YASchedulerImpl(NioUtil.reduceSelectorGarbage(Selector.open()),
                            log);
                    } catch (IOException x) {
                        sched = null;
                        CommBase.signalFatalError("NIODriver.init: can't create scheduler", x);
                    }
                    for (int i = 0; i < NUM_INITIAL_THREADS; ++i) {
                        addThread();
                    }
                }
                initialized = true;
            }
        }

    }

    /**
     * Shut down, and wait for all threads to terminate. This method is really just for testing;
     * it's a bad idea to do this in production because waiting for threads to terminate is prone to
     * deadlocks. If desired, though, it can be called from an AbstractService shutdown hook
     * installed in init().
     */
    public static boolean shutdown(long maxWait) {
        synchronized (lock) {
            if (!initialized)
                return true;

            if (useFastNIODriver) {
                if (driver.shutdown(maxWait)) {
                    initialized = false;
                    log.info().append("NIODriver.shutdown: finished").endl();
                    return true;
                } else {
                    return false;
                }
            } else {
                long deadline = System.currentTimeMillis() + maxWait, remain = maxWait;
                stopped = true;
                lock.notifyAll();
                // force the scheduler to wake up
                sched.installJob(new TimedJob() {
                    public void timedOut() {}
                }, 0);
                while (created != destroyed) {
                    try {
                        log.info().append("NIODriver.shutdown: waiting for threads to terminate: ")
                            .append(summary()).endl();
                        lock.wait(Math.max(remain, 0));
                    } catch (InterruptedException x) {
                        // ignore
                    }
                    if ((remain = deadline - System.currentTimeMillis()) < 0) {
                        return false;
                    }
                }
                sched.close();
                log.info().append("NIODriver.shutdown: finished").endl();
                leader = null;
                sched = null;
                initialized = stopped = false;
                created = destroyed = 0;
                available.set(0);
                return true;
            }
        }
    }

    /**
     * Return the scheduler used by the NIO driver
     */
    public static Scheduler getScheduler() {
        return sched;
    }

    /**
     * Return the scheduler used by the NIO driver
     */
    public static Logger getLogger() {
        return log;
    }

    /**
     * add a thread to the pool
     *
     * NOTE: caller must hold the lock!
     *
     * NOTE: We increment the "waiting" variable *before* we start the new thread, and then make
     * sure to correct it in the first iteration of the thread loop. This prevents a race in which
     * we handoff() method creates too many threads, because it keeps getting called before the
     * first thread it creates can get started.
     */
    private static void addThread() {
        if (created == HARD_MAX_THREADS) {
            log.fatal().append("NIODriver: exceeded maximum thread pool limit: ").append(summary())
                .endl();
            LogCrashDump.logCrashDump(log);
            CommBase.signalFatalError("NIODriver: exceeded maximum thread pool limit: " + summary(),
                new Throwable());
        }
        Thread thread = new Thread(new NIODriver());
        thread.setDaemon(true);
        thread.setName("NIODriver-" + created);
        created++;
        available.incrementAndGet();
        log.info().append("Thread ").append(thread.getName()).append(" is starting: ")
            .append(summary()).endl();
        thread.start();
    }

    /**
     * the threads' run method just does an endless loop, trying to become the leader whenever it
     * can
     */
    public void run() {
        Thread me = Thread.currentThread();
        STOP: {
            while (true) {
                synchronized (lock) {
                    while (leader != me) {
                        if (stopped) {
                            destroyed++;
                            log.info().append("Thread ").append(me.getName())
                                .append(" is terminating: ").append(summary()).endl();
                            lock.notifyAll();
                            break STOP;
                        } else if (leader == null) {
                            if (log.isDebugEnabled()) {
                                log.debug().append("Thread ").append(me.getName())
                                    .append(" is assuming leadership").endl();
                            }
                            leader = me;
                        } else {
                            try {
                                if (log.isDebugEnabled()) {
                                    log.debug().append("Thread ").append(me.getName())
                                        .append(" is waiting ").append(summary()).endl();
                                }
                                lock.wait();
                                if (log.isDebugEnabled()) {
                                    log.debug().append("Thread ").append(me.getName())
                                        .append(" has awoken ").append(summary()).endl();
                                }
                            } catch (InterruptedException x) {
                                // ignore
                            }
                        }
                    }
                }
                try {
                    available.decrementAndGet();
                    sched.work(WORK_TIMEOUT, handoffProc);
                    available.incrementAndGet();
                } catch (Throwable x) {
                    synchronized (lock) {
                        destroyed++;
                        log.fatal(x).append("Thread ").append(me.getName())
                            .append(" is terminating on a fatal exception: ").append(summary())
                            .endl();
                        lock.notifyAll();
                    }

                    NIODriver.shutdown(5000);
                    CommBase.signalFatalError("Unhandled throwable from NIO scheduler", x);
                    break STOP;
                }
            }
        }
    }

    // whitebox test support methods
    public static int junit_getWaiting() {
        if (useFastNIODriver) {
            return driver.junit_getWaiting();
        } else {
            return available.get();
        }
    }

    public static int junit_getCreated() {
        if (useFastNIODriver) {
            return driver.junit_getCreated();
        } else {
            return created;
        }
    }

    public static int junit_getDestroyed() {
        if (useFastNIODriver) {
            return driver.junit_getDestroyed();
        } else {
            return destroyed;
        }
    }

    // ################################################################

}
