/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.exceptions.QueryCancellationException;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.NotificationStepSource;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.FunctionalInterfaces;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A multithreaded resource to execute data driven models.
 *
 * @param <DATATYPE> data type
 */
public abstract class ModelFarmBase<DATATYPE> implements ModelFarm {

    private static final Logger log = LoggerFactory.getLogger(ModelFarmBase.class);

    /**
     * Number of threads created so far. Just used for generating thread names.
     */
    private static int modelFarmNThreads = 0;
    private static final AtomicInteger nModelFarms = new AtomicInteger(0);
    private final int modelFarmN = nModelFarms.getAndIncrement();

    /**
     * Run state of the farm.
     */
    public enum State {
        WAITING, RUNNING, SHUTDOWN, TERMINATING, TERMINATED
    }

    /**
     * An operation that uses data from Deephaven {@link io.deephaven.db.tables.Table Tables}, using either
     * {@link io.deephaven.db.v2.sources.ColumnSource#getPrev} or {@link io.deephaven.db.v2.sources.ColumnSource#get})
     * depending on the value of the argument to {@link #retrieveData}.
     */
    @FunctionalInterface
    interface QueryDataRetrievalOperation {

        /**
         * Performs an operation using data from a query.
         *
         * @param usePrev Whether to use the previous data at a given index when retrieving data (i.e. if {@code true},
         *        use {@link io.deephaven.db.v2.sources.ColumnSource#getPrev} instead of
         *        {@link io.deephaven.db.v2.sources.ColumnSource#get}).
         */
        void retrieveData(boolean usePrev);

    }

    /**
     * Type of locking used when loading data from the data manager.
     */
    public enum GetDataLockType {
        /**
         * The LTM lock is already held.
         */
        LTM_LOCK_ALREADY_HELD,
        /**
         * Acquire the LTM lock.
         */
        LTM_LOCK,
        /**
         * Acquire an LTM read lock.
         */
        LTM_READ_LOCK,
        /**
         * Use the (usually) lock-free snapshotting mechanism.
         */
        SNAPSHOT
    }

    /**
     * The model to which data should be passed.
     */
    protected final Model<DATATYPE> model;
    private final ThreadGroup threadGroup;
    private final Set<Thread> threads = new LinkedHashSet<>();

    /**
     * This model farm's state. Updated under lock on this {@code ModelFarmBase} instance. Should be used with
     * {@link #setState} and {@link #getState}
     */
    private State state = State.WAITING;

    private class Worker implements Runnable {
        @Override
        public void run() {
            synchronized (ModelFarmBase.this) {
                // The worker threads should be added to the list of threads before starting.
                Assert.assertion(threads.contains(Thread.currentThread()), "threads.contains(Thread.currentThread())");
            }

            try {
                while (true) {
                    try {
                        execute();
                    } catch (InterruptedException e) {
                        log.warn().append("ModelFarm worker thread interrupted.").endl();
                    } catch (Exception e) {
                        log.error(e).append("Exception in ModelFarm worker thread.").endl();
                        final StringWriter sw = new StringWriter();
                        final PrintWriter pw = new PrintWriter(sw);
                        e.printStackTrace(pw);
                        pw.close();
                        log.error().append("Exception in ModelFarm worker thread stack trace. \n").append(sw.toString())
                                .endl();
                        throw new RuntimeException(e);
                    }

                    final State state = getState();

                    // During shutdown, keep pulling items from the queue until it is empty.
                    if ((state == State.SHUTDOWN && isQueueEmpty()) || state == State.TERMINATING
                            || state == State.TERMINATED) {
                        log.warn().append("ModelFarm worker thread exiting. state=").append(state.toString())
                                .append(" isQueueEmpty=").append(isQueueEmpty()).endl();
                        return;
                    }
                }
            } finally {
                synchronized (ModelFarmBase.this) {
                    threads.remove(Thread.currentThread());
                    // Set the ModelFarm as terminated if this is the last thread to finish.
                    final boolean threadsEmpty = threads.isEmpty();

                    if (threadsEmpty && (ModelFarmBase.this.state == State.SHUTDOWN
                            || ModelFarmBase.this.state == State.TERMINATING)) {
                        setState(State.TERMINATED);
                    }
                }
            }
        }
    }

    /**
     * Create a multithreaded resource to execute data driven models.
     *
     * @param nThreads number of worker threads.
     * @param model model to execute.
     */
    @SuppressWarnings("WeakerAccess")
    protected ModelFarmBase(final int nThreads, final Model<DATATYPE> model) {
        this.model = Require.neqNull(model, "model");
        this.threadGroup = initializeThreadGroup(Require.gtZero(nThreads, "nThreads"), this.threads);
    }

    private ThreadGroup initializeThreadGroup(final int nThreads, final Set<Thread> threads) {
        // synchronized for modelFarmNThreads.
        synchronized (ModelFarmBase.class) {
            final ThreadGroup threadGroup = new ThreadGroup("ModelFarm");

            for (int i = 0; i < nThreads; i++) {
                final String threadName = "ModelFarm_" + modelFarmN + "_Thread_" + (modelFarmNThreads++);
                threads.add(new Thread(threadGroup, new Worker(), threadName));
            }

            return threadGroup;
        }
    }

    /**
     * Executes the next task in the work queue.
     *
     * @throws InterruptedException if interrupted while executing
     */
    protected abstract void execute() throws InterruptedException;

    /**
     * Interface for getting the most recent row data for a unique identifier.
     *
     * @param <KEYTYPE> unique ID key type
     * @param <DATATYPE> data type
     */
    @FunctionalInterface
    interface MostRecentDataGetter<KEYTYPE, DATATYPE> {
        /**
         * Gets the most recent row data for a unique identifier.
         *
         * @param key unique identifier
         * @return most recent row data for the unique identifier, or null, if there is no data for the unique
         *         identifier.
         */
        DATATYPE get(final KEYTYPE key);
    }

    /**
     * Returns a {@code ThrowingConsumer} that takes a {@link QueryDataRetrievalOperation}, acquires a
     * {@link LiveTableMonitor} lock based on the specified {@code lockType}, then executes the {@code FitDataPopulator}
     * with the appropriate value for usePrev.
     *
     * @param lockType The way of acquiring the {@code LiveTableMonitor} lock.
     * @return A function that runs a {@link }
     */
    @SuppressWarnings("WeakerAccess")
    protected static FunctionalInterfaces.ThrowingBiConsumer<QueryDataRetrievalOperation, NotificationStepSource, RuntimeException> getDoLockedConsumer(
            final GetDataLockType lockType) {
        switch (lockType) {
            case LTM_LOCK_ALREADY_HELD:
                return (queryDataRetrievalOperation, source) -> queryDataRetrievalOperation.retrieveData(false);
            case LTM_LOCK:
                return (queryDataRetrievalOperation, source) -> LiveTableMonitor.DEFAULT.exclusiveLock()
                        .doLocked(() -> queryDataRetrievalOperation.retrieveData(false));
            case LTM_READ_LOCK:
                return (queryDataRetrievalOperation, source) -> LiveTableMonitor.DEFAULT.sharedLock()
                        .doLocked(() -> queryDataRetrievalOperation.retrieveData(false));
            case SNAPSHOT:
                return (queryDataRetrievalOperation, source) -> {
                    try {
                        ConstructSnapshot.callDataSnapshotFunction("ModelFarmBase.getData(SNAPSHOT)",
                                ConstructSnapshot.makeSnapshotControl(false, source),
                                (usePrev, beforeClockValue) -> {
                                    queryDataRetrievalOperation.retrieveData(usePrev);
                                    return true; // This indicates that the snapshot ran OK, not that the data is OK.
                                });
                    } catch (QueryCancellationException e) {
                        log.warn(e).append(
                                "ModelFarmBase.getData(SNAPSHOT): QueryCancellationException.  The ModelFarm is probably shutting down.")
                                .endl();
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported lockType: " + lockType);
        }
    }

    /**
     * Gets the current run state of the model farm. The state is {@code null} before the model farm has started.
     *
     * @return current run state of the model farm.
     */
    @SuppressWarnings("WeakerAccess")
    protected final synchronized State getState() {
        return state;
    }

    private synchronized void setState(State state) {
        final boolean changed = this.state != state;
        this.state = Require.neqNull(state, "state");

        if (changed) {
            // notify extending classes that the state has changed, so recalculation may be necessary
            this.notifyAll();
        }
    }

    @Override
    public final synchronized void start() {
        if (state != State.WAITING) {
            throw new IllegalStateException("Start may only be called on an unstarted ModelFarm. state=" + state);
        }

        setState(State.RUNNING);

        for (Thread thread : threads) {
            thread.start();
        }

        modelFarmStarted();
    }

    /**
     * Method called after the model farm threads have been started. Implementing classes can override this to perform
     * additional setup (e.g. creating and starting listeners). The default implementation does nothing.
     */
    protected abstract void modelFarmStarted();


    @Override
    public final synchronized void shutdown() {
        switch (state) {
            case SHUTDOWN:
            case TERMINATING:
            case TERMINATED:
                return;
            case WAITING:
            case RUNNING:
                log.info().append("ModelFarm shutting down...").endl();
                setState(State.SHUTDOWN);
                break;
            default:
                throw new IllegalStateException("State is not being handled by the switch! state=" + state);
        }
    }

    /**
     * Attempt to terminate the ModelFarm by {@link #shutdown() shutting it down} and interrupting all worker threads.
     */
    @Override
    public final synchronized void terminate() {
        if (state != State.TERMINATING && state != State.TERMINATED) {
            setState(State.TERMINATING);
            threadGroup.interrupt();
        }
    }

    @Override
    public final boolean awaitTermination() {
        return awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public final boolean awaitTermination(final long timeout, final TimeUnit unit) {

        synchronized (this) {
            switch (state) {
                case WAITING:
                    setState(State.TERMINATED);
                    return true;
                case RUNNING:
                    shutdown();
                    break;
                case SHUTDOWN:
                case TERMINATING:
                    break;
                case TERMINATED:
                    return true;
                default:
                    throw new IllegalStateException("State is not being handled by the switch! state=" + state);
            }

            Require.eqTrue(isShutdown(), "isShutdown()");
        }

        final long timeoutMillis =
                timeout == Long.MAX_VALUE ? Long.MAX_VALUE : System.currentTimeMillis() + unit.toMillis(timeout);
        boolean allThreadsTerminated = false;

        while (!allThreadsTerminated && System.currentTimeMillis() < timeoutMillis) {
            synchronized (ModelFarmBase.this) {
                if (!threads.isEmpty()) {
                    try {
                        // Wait for the state to change. (The last thread to exit will update the state to TERMINATED.)
                        ModelFarmBase.this.wait(timeoutMillis - System.currentTimeMillis());

                        if (threads.isEmpty()) {
                            allThreadsTerminated = true;
                        }
                    } catch (InterruptedException e) {
                        if (threads.isEmpty()) {
                            allThreadsTerminated = true;
                        } else {
                            throw new RuntimeException("Interrupted while awaiting ModelFarm termination.", e);
                        }
                    }
                }
            }
        }

        if (allThreadsTerminated) {
            Assert.eq(getState(), "getState()", State.TERMINATED);
            log.warn().append("ModelFarm all threads terminated.").endl();
        } else {
            log.warn().append("ModelFarm timed out waiting for threads to terminate.").endl();
        }

        return allThreadsTerminated;
    }

    private boolean isShutdown() {
        final State state = getState();

        switch (state) {
            case SHUTDOWN:
            case TERMINATING:
            case TERMINATED:
                return true;
            case WAITING:
            case RUNNING:
                return false;
        }

        throw new IllegalStateException("State is not being handled by the switch! state=" + state);
    }

    @Override
    public final void shutdownAndAwaitTermination() {
        shutdownAndAwaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public final boolean shutdownAndAwaitTermination(final long timeout, final TimeUnit unit) {
        shutdown();
        return awaitTermination(timeout, unit);
    }

    /**
     * Returns true if the model farm queue is empty and false if the queue contains elements to execute.
     *
     * @return true if the model farm queue is empty and false if the queue contains elements to execute.
     */
    protected abstract boolean isQueueEmpty();

    @Override
    public String toString() {
        return "ModelFarm" + modelFarmN + "_" + this.getClass().getSimpleName();
    }
}
