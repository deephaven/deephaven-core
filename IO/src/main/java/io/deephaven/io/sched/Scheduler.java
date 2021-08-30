/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.sched;

import io.deephaven.base.Procedure;

import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * This class provides a singleton wrapper for scheduling invocations of multiple Job instances from
 * a single thread. Job are scheduled in accordance with an interest set on a java.nio.Channel,
 * deadline based time scheduling, and/or custom criteria defined by the Jobs' implementation of the
 * ready() method.
 *
 * Jobs are instantiated by the application and made known to the scheduler by one of the install()
 * methods. Once the job is installed, the scheduler will call exactly one of its invoke(),
 * timedOut() or cancelled() methods exactly once. After this, the scheduler forgets about the job
 * completely, unless the application installs it again.
 */
public interface Scheduler {

    // --------------------------------------------------------------------------
    // public interface
    // --------------------------------------------------------------------------

    /**
     * Return the scheduler's idea of the current time.
     */
    public long currentTimeMillis();

    /**
     * Install a job in association with a channel and an interest set.
     */
    public void installJob(Job job, long deadline, SelectableChannel channel, int interest);

    /**
     * Install a job with only an associated deadline (removing any channel association)
     */
    public void installJob(Job job, long deadline);

    /**
     * Cancel a job, making the scheduler forget it completely..
     */
    public void cancelJob(Job job);

    /**
     * Wait for jobs to become ready, then invoke() them all. This method will form the core of the
     * main loop of a scheduler-driven application. The method first waits until:
     *
     * -- the given timeout expires, -- the earliest job-specific timeout expires, or -- one or more
     * jobs becomes ready
     *
     * If jobs have become ready, then the entire ready set will be invoked. If any job throws an
     * uncaught exception, the job's terminated() method will be called and the job deregistered.
     * This does not abort the invocation of the remaining jobs. The return value is then the number
     * of jobs that were invoked.
     *
     * If no jobs are ready and any job-specific timeouts expire, the associated jobs' timedOut()
     * methods are called. The return value is the negative of the number of expired timeouts.
     *
     * If the time given by the timeout argument expires, then zero is returned.
     *
     * Note that this method is not synchronized. The application must ensure that it is never
     * called concurrently by more than one thread.
     *
     * @return true, if some job was dispatched
     */
    public boolean work(long timeout, Procedure.Nullary handoff);

    /**
     * Shut down the scheduler, calling close() on the underlying Selector.
     */
    public void close();

    /**
     * Return true if the scheduler is closing or closed.
     */
    public boolean isClosed();

    // --------------------------------------------------------------------------
    // test support methods
    // --------------------------------------------------------------------------

    /**
     * Return a reference to the selector
     */
    public Selector junitGetSelector();

    /**
     * Return all jobs known to the scheduler, in whatever state.
     */
    public Set<Job> junitGetAllJobs();

    /**
     * Return the contents of the timeout queue, in deadline order
     * 
     * @return the jobs in the timeout queue
     */
    public ArrayList<Job> junitGetTimeoutQueue();

    /**
     * Return the selection keys currently known to the scheduler.
     */
    public ArrayList<SelectionKey> junitGetAllKeys();

    /**
     * Return the selection keys currently known to the scheduler.
     */
    public ArrayList<SelectionKey> junitGetReadyKeys();

    /**
     * Return a map containing all channels and the jobs to which they are associated.
     */
    public Map<SelectableChannel, Job> junitGetChannelsAndJobs();

    /**
     * Return true if the timeout queue invariant holds.
     */
    public boolean junitTestTimeoutQueueInvariant();

    public class Null implements Scheduler {
        @Override
        public long currentTimeMillis() {
            return 0;
        }

        @Override
        public void installJob(Job job, long deadline, SelectableChannel channel, int interest) {}

        @Override
        public void installJob(Job job, long deadline) {}

        @Override
        public void cancelJob(Job job) {}

        @Override
        public boolean work(long timeout, Procedure.Nullary handoff) {
            return false;
        }

        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public Selector junitGetSelector() {
            return null;
        }

        @Override
        public Set<Job> junitGetAllJobs() {
            return null;
        }

        @Override
        public ArrayList<Job> junitGetTimeoutQueue() {
            return null;
        }

        @Override
        public ArrayList<SelectionKey> junitGetAllKeys() {
            return null;
        }

        @Override
        public ArrayList<SelectionKey> junitGetReadyKeys() {
            return null;
        }

        @Override
        public Map<SelectableChannel, Job> junitGetChannelsAndJobs() {
            return null;
        }

        @Override
        public boolean junitTestTimeoutQueueInvariant() {
            return false;
        }
    }

    public final class ExecutorAdaptor implements Executor {
        final Scheduler scheduler;

        public ExecutorAdaptor(final Scheduler scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public void execute(final Runnable runnable) {
            scheduler.installJob(new TimedJob() {
                @Override
                public final void timedOut() {
                    runnable.run();
                }
            }, 0);
        }
    }
}
