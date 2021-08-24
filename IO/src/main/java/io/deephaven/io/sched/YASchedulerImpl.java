/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.sched;

import io.deephaven.base.Procedure;
import io.deephaven.base.RingBuffer;
import io.deephaven.base.stats.*;
import io.deephaven.io.logger.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.*;
import java.util.*;

/**
 * Yet Another implementation of the Scheduler interface -- the best one yet.
 *
 * This class provides a singleton wrapper for scheduling invocations of multiple Job instances from
 * a single thread. Job are scheduled in accordance with an interest set on a java.nio.Channel,
 * deadline based time scheduling, and/or custom criteria defined by the Jobs' implementation of the
 * ready() method.
 *
 * Jobs are instantiated by the application and made known to the scheduler by one of the
 * installJob() methods. A previously installed job can be removed from the scheduler with the
 * cancelJob() method. The installJob() and cancelJob() methods are thread-safe. It is allowed to
 * call installJob() on a job that is already installed, or cancelJob() on a job that is not current
 * in the scheduler. In the former case, the channel and/or deadline will be updated accordingly; in
 * the latter, the call will be ignored.
 *
 * Once the job is installed, the scheduler promises to call exactly one of its invoke(), timedOut()
 * or cancelled() methods exactly once. The invoke() method will be called only if the job was
 * (last) installed with a channel and non-zero interest set. The timedOut() method can be called
 * for any job, since all jobs have an associated deadline (although the timeout value can be set to
 * Integer.MAX_VALUE to make if effectively infinite). The cancelled() method is called only if the
 * job is removed by a cancelJob() call before either the channe is ready or the deadline expires.
 *
 * After the job is called back, the scheduler forgets about the job completely, unless the
 * application installs it again. That is, from the scheduler's point of view *all* jobs are
 * one-shots. This design is based on the observation that it is easier to reschedule jobs on every
 * invocation in the style of a tail-recursive loop, as opposed to maintaining persistent state in
 * the scheduler.
 *
 * The application must drive the scheduler by calling the work() method in a loop. The work()
 * method is *not* thread-safe; the application must either call it from a single thread or
 * synchronize calls accordingly.
 */
public class YASchedulerImpl implements Scheduler {

    /** the scheduler name, for debug and stats output */
    protected final String name;

    /** the java.nio.Selector instance */
    private final Selector selector;

    /** the logger */
    protected final Logger log;

    /** lock for internal state */
    private final Object stateLock = new Object();

    /**
     * if non-zero, there is a select() in progress that will terminate at the specified deadline
     */
    private long selectingTill = 0;

    private volatile boolean spinWakeSelector = false;

    /** the update clock for this scheduler */
    private long updateClock = 1;

    /** the waiting jobs, ordered by deadline */
    private final JobStateTimeoutQueue timeoutQueue;

    /** invokable/timed-out jobs are stored here */
    private RingBuffer<JobState> dispatchQueue = new RingBuffer<JobState>(128);

    /** the list of jobs which might have changed since the last update() call */
    private ArrayList<JobState> changedStates = new ArrayList<JobState>(128);

    /** add a state to the changedStates list */
    private boolean changedState(JobState state) {
        if (state.updateClock < updateClock) {
            state.updateClock = updateClock;
            changedStates.add(state);
            return true;
        }

        // Assert.eqTrue(isInChangedStates(state), "isInChangedStates(state)"); // temporary

        return false;
    }

    private boolean isInChangedStates(JobState state) {
        final int L = changedStates.size();
        for (int i = 0; i < L; ++i) {
            if (state == changedStates.get(i)) {
                return true;
            }
        }
        return false;
    }

    /** if there are lots of tiny jobs, taking timing measurements may be time consuming. */
    private final boolean doTimingStats;

    private final boolean doSpinSelect;

    /** time base for loop duration measurements */
    private long lastNanos = 0;

    private void mark(Value v) {
        if (doTimingStats) {
            long t = System.nanoTime();
            if (lastNanos != 0) {
                v.sample((t - lastNanos + 500) / 1000);
            }
            lastNanos = t;
        }
    }

    /** have we been closed? */
    private volatile boolean isClosed = false;

    // statistics
    private Value invokeCount;
    private Value timeoutCount;
    private Value selectDuration;
    private Value workDuration;
    private Value gatheredDuration;
    private Value channelInstalls;
    private Value timedInstalls;
    private Value jobCancels;
    private Value jobUpdates;
    private Value keyUpdates;
    private Value keyOrphans;
    private Value selectorWakeups;
    private Value channelInterestWakeups;
    private Value channelTimeoutWakeups;
    private Value plainTimeoutWakeups;
    private Value cancelWakeups;

    /**
     * The constructor.
     */
    public YASchedulerImpl(Selector selector, Logger log) throws IOException {
        this("Scheduler", selector, log);
    }

    public YASchedulerImpl(String name, Selector selector, Logger log) throws IOException {
        this(name, selector, log, true, false);
    }

    public YASchedulerImpl(String name, Selector selector, Logger log, boolean doTimingStats,
        boolean doSpinSelect) {
        this.name = name;
        this.selector = selector;
        this.log = log;
        this.doTimingStats = doTimingStats;
        this.doSpinSelect = doSpinSelect;

        this.timeoutQueue = new JobStateTimeoutQueue(log, 1024);

        this.invokeCount = Stats.makeItem(name, "invokeCount", Counter.FACTORY,
            "The number of jobs invoked for I/O").getValue();
        this.timeoutCount = Stats.makeItem(name, "timeoutCount", Counter.FACTORY,
            "The number of jobs that have timed out").getValue();
        this.selectDuration = Stats.makeItem(name, "SelectDuration", State.FACTORY,
            "The number of microseconds spent in select()").getValue();
        this.workDuration = Stats.makeItem(name, "WorkDuration", State.FACTORY,
            "The number of microseconds between successive select() calls").getValue();
        this.gatheredDuration = Stats.makeItem(name, "GatheredDuration", State.FACTORY,
            "The number of microseconds jobs spend waiting after being gathered").getValue();
        this.channelInstalls = Stats.makeItem(name, "channelInstalls", Counter.FACTORY,
            "The number of installJob() calls with a channel").getValue();
        this.timedInstalls = Stats.makeItem(name, "timedInstalls", Counter.FACTORY,
            "The number of installJob() calls with just a timeout").getValue();
        this.jobCancels = Stats.makeItem(name, "jobCancels", Counter.FACTORY,
            "The number of cancelJob() calls").getValue();
        this.jobUpdates = Stats.makeItem(name, "jobUpdates", Counter.FACTORY,
            "The number of updates applied to the job state pre- and post-select").getValue();
        this.keyUpdates = Stats.makeItem(name, "keyUpdates", Counter.FACTORY,
            "The number of times an NIO SelectionKey was updated with non-zero interest")
            .getValue();
        this.keyOrphans = Stats.makeItem(name, "keyOrphans", Counter.FACTORY,
            "The number of times an NIO SelectionKey's interest was cleared").getValue();
        this.selectorWakeups = Stats.makeItem(name, "selectorWakeups", Counter.FACTORY,
            "The number of times the selector had to be woken up").getValue();

        this.channelInterestWakeups = Stats
            .makeItem(name, "channelInterestWakeups", Counter.FACTORY,
                "The number of selector wakeups due to a change in a channel's interest set")
            .getValue();
        this.channelTimeoutWakeups = Stats.makeItem(name, "channelTimeoutWakeups", Counter.FACTORY,
            "The number of selector wakeups due to a channel's timeout becoming the earliest")
            .getValue();
        this.plainTimeoutWakeups = Stats.makeItem(name, "plainTimeoutWakeups", Counter.FACTORY,
            "The number of selector wakeups due to a plain timeout becoming the earliest")
            .getValue();
        this.cancelWakeups = Stats.makeItem(name, "cancelWakeups", Counter.FACTORY,
            "The number of selector wakeups due to a job cancellation").getValue();
    }

    /**
     * Return the scheduler's idea of the current time.
     */
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Install a job in association with a channel and an interest set.
     */
    public void installJob(Job job, long deadline, SelectableChannel channel, int interest) {
        synchronized (stateLock) {
            JobState state = job.makeStateFor(this);
            SelectionKey key = channel.keyFor(selector);

            // see if we will need to wake up the selector
            boolean wakeup = false;
            if (key == null || !key.isValid()) {
                wakeup = true;
            } else if (deadline < selectingTill) {
                wakeup = true;
                channelTimeoutWakeups.sample(1);
            } else if (key.interestOps() != interest
                && (channel != state.nextChannel || interest != state.nextOps)) {
                wakeup = true;
                channelInterestWakeups.sample(1);
            }

            state.nextChannel = channel;
            state.nextOps = interest;
            state.nextDeadline = deadline;
            state.cancelled = false;
            state.forgotten = false;
            changedState(state);

            if (log.isDebugEnabled()) {
                log.debug().append(name).append(" installed job ").append(job)
                    .append(", d=").append(deadline)
                    .append(", ni=").append(state.nextOps)
                    // .append(", k=").append(key)
                    .append(", ki=").append((key == null || !key.isValid() ? 0 : key.interestOps()))
                    .append(", w=").append(wakeup)
                    .endl();
            }

            if (wakeup) {
                maybeWakeSelector();
            }

            // must always wake if doing spin select since we aren't setting selectingTill
            else if (doSpinSelect) {
                spinWakeSelector = true;
            }

            channelInstalls.sample(1);
        }
    }

    /**
     * Install a job with only an associated deadline (removing any channel association)
     */
    public void installJob(Job job, long deadline) {
        synchronized (stateLock) {
            JobState state = job.makeStateFor(this);
            state.nextChannel = null;
            state.nextOps = 0;
            state.nextDeadline = deadline;
            state.cancelled = false;
            state.forgotten = false;
            final boolean changed = changedState(state);

            // Note: We don't need to be concerned with waking up due to channelInterest changes,
            // since
            // we would have to be reducing the interest set which can only lead to a later wakeup
            // time.

            // if the new deadline is earlier than the current top, wake up the selector
            boolean wakeup = false;
            if (deadline < selectingTill) {
                plainTimeoutWakeups.sample(1);
                maybeWakeSelector();
            }

            // must always wake if doing spin select since we aren't setting selectingTill
            else if (doSpinSelect) {
                spinWakeSelector = true;
            }

            if (log.isDebugEnabled()) {
                log.debug().append(name).append(" installed job ").append(job)
                    .append(", d=").append(deadline)
                    .append(", w=").append(wakeup)
                    .append(", c=").append(changed)
                    .endl();
            }

            timedInstalls.sample(1);
        }
    }

    /**
     * Cancel a job's selection key with the scheduler.
     *
     * @param job the job to be cancelled.
     */
    public void cancelJob(Job job) {
        synchronized (stateLock) {
            if (log.isDebugEnabled()) {
                log.debug().append(name).append(" explicitly cancelling ").append(job)
                    .append(" in YAScheduler.cancelJob").endl();
            }
            JobState state = job.getStateFor(this);
            if (state != null) {
                state.nextChannel = null;
                state.nextOps = 0;
                state.nextDeadline = 0;
                state.cancelled = true;
                state.forgotten = false;
                changedState(state);

                if (state.waitChannel != null) {
                    cancelWakeups.sample(1);
                    maybeWakeSelector();
                }
                jobCancels.sample(1);
            }
        }
    }

    /**
     * drop the association of a state with a channel
     */
    private void dropChannel(JobState state) {
        if (state.waitChannel != null) {
            SelectionKey key = state.waitChannel.keyFor(selector);
            try {
                if (key != null && key.isValid() && key.attachment() == state) {
                    key.attach(null);
                    if (key.interestOps() != 0) {
                        key.interestOps(0);
                        if (log.isDebugEnabled()) {
                            log.debug().append(name).append(" setting interest on orphaned key ")
                                .append(key.toString()).append(" to 0").endl();
                        }
                        keyUpdates.sample(1);
                    }
                }
            } catch (CancelledKeyException x) {
                // ignore it
                if (log.isDebugEnabled()) {
                    log.info().append(name)
                        .append(" got CancelledKeyException while dropping channel ")
                        .append(state.waitChannel.toString()).endl();
                }
            }
            state.waitChannel = null;
        }
    }

    /**
     * associate a channel with a state
     */
    private boolean grabChannel(JobState state) {
        try {
            SelectionKey key = state.nextChannel.keyFor(selector);
            if (key == null) {
                key = state.nextChannel.register(selector, state.nextOps, state);
                log.debug().append(name).append(" update ").append(state.job)
                    .append(": registered channel ").append(state.nextChannel.toString())
                    .append(", ni=").append(state.nextOps)
                    .append(", k=").append(key.toString())
                    .endl();
            } else {
                key.attach(state);
                if (key.interestOps() != state.nextOps) {
                    if (log.isDebugEnabled()) {
                        log.debug().append(name).append(" update ").append(state.job)
                            .append(": setting interest on key ").append(key.toString())
                            .append(" to ").append(state.nextOps)
                            .endl();
                    }
                    key.interestOps(state.nextOps);
                    keyUpdates.sample(1);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug().append(name).append(" update ").append(state.job)
                            .append(": interest on key ").append(key.toString())
                            .append(" already at ").append(state.nextOps)
                            .endl();
                    }
                }
            }
            if (state.waitChannel != null && state.waitChannel != state.nextChannel) {
                SelectionKey waitKey = state.waitChannel.keyFor(selector);
                if (waitKey != null && waitKey.attachment() == state) {
                    try {
                        waitKey.interestOps(0);
                    } catch (CancelledKeyException x) {
                        // ignore this
                    }
                }
            }
            state.waitChannel = state.nextChannel;
            return true;
        } catch (ClosedChannelException x) {
            // fall through
        } catch (CancelledKeyException x) {
            // fall through
        }
        state.waitChannel = null;
        log.error().append(name).append(" tried to register ").append(state.job)
            .append(" on closed channel ").append(state.nextChannel.toString()).endl();
        return false;
    }

    /**
     * Apply changes to the job states.
     *
     * NOTE: assumes that stateLock is held
     */
    private void update() {
        // DO NOT USE FOREACH HERE AS IT CREATES AN INTERATOR -> No Allocation changes
        int size = changedStates.size();
        for (int i = 0; i < size; i++) {
            JobState state = changedStates.get(i);
            jobUpdates.sample(1);

            if (log.isDebugEnabled()) {
                SelectionKey key = null;
                if (state.nextChannel != null) {
                    key = state.nextChannel.keyFor(selector);
                }
                log.debug().append(name).append(" updating job ").append(state.job)
                    .append(", d=").append(state.nextDeadline)
                    .append(", ni=").append(state.nextOps)
                    .append(", k=").append(key == null ? "null" : key.toString())
                    .append(", ki=").append(key == null || !key.isValid() ? 0 : key.interestOps())
                    .endl();
            }

            if (state.gathered) {
                // job is waiting to be invoked; leave it alone
            } else if (state.nextChannel != null && state.nextOps != 0) {
                if (!grabChannel(state)) {
                    log.error().append(name).append(" cancelling ").append(state.job)
                        .append(" after failed I/O registration").endl();
                    timeoutQueue.remove(state);
                    state.cancelled = true;
                    dispatchQueue.add(state);
                } else {
                    timeoutQueue.enter(state, state.nextDeadline);
                }
            } else if (state.forgotten) {
                dropChannel(state);
                timeoutQueue.remove(state);
            } else if (state.cancelled) {
                dropChannel(state);
                timeoutQueue.remove(state);
                if (log.isDebugEnabled()) {
                    log.debug().append(name).append(" cancelling ").append(state.job)
                        .append(" from update()").endl();
                }
                state.cancelled = true;
                dispatchQueue.add(state);
            } else {
                dropChannel(state);
                timeoutQueue.enter(state, state.nextDeadline);
            }

            state.forgotten = true;
            state.nextChannel = null;
            state.nextOps = 0;
            state.nextDeadline = 0;

            assert state.waitChannel == null
                || state.waitChannel.keyFor(selector).attachment() == state;
        }
        if (log.isDebugEnabled()) {
            log.debug().append(name).append(" updated ").append(changedStates.size())
                .append(" jobs").endl();
        }
        changedStates.clear();
        updateClock++;
    }

    /**
     * compute the timeout value for the next select() call
     *
     * NOTE: assumes that stateLock is held
     */
    private long computeTimeout(long now, long timeout) {
        if (!dispatchQueue.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug().append(name)
                    .append(" update: dispatch queue is not empty, setting timeout to zero").endl();
            }
            timeout = 0;
        } else if (!timeoutQueue.isEmpty()) {
            JobState next = timeoutQueue.top();
            long remain = next.deadline - now;
            if (log.isDebugEnabled()) {
                log.debug().append(name).append(" update: next timeout due in ").append(remain)
                    .append(" millis: ").append(next.job).endl();
            }
            timeout = Math.max(0, Math.min(timeout, remain));
        }
        return timeout;
    }

    /**
     * Wait for something to happen
     */
    private void select(long timeout) {
        try {
            if (log.isDebugEnabled()) {
                log.debug().append(name).append(" calling select(").append(timeout).append(")")
                    .endl();
            }

            mark(workDuration);

            if (timeout > 0) {
                selector.select(timeout);
            } else {
                selector.selectNow();
            }

            mark(selectDuration);
        } catch (IOException x) {
            if (java.util.regex.Pattern.matches(".*Operation not permitted.*", x.toString())) {
                // There is a documented bug
                // (http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6481709) in some
                // versions of the epoll selector which causes occasional "Operation not permitted"
                // errors to be
                // thrown.
                log.warn().append(name).append(
                    " Ignoring 'Operation not permitted' exception, see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6481709")
                    .endl();
            } else {
                if (!isClosed()) {
                    log.fatal(x).append(name).append(" Unexpected IOException in select(): ")
                        .append(x.getMessage()).endl();
                    System.exit(1);
                }
            }
        } catch (ClosedSelectorException x) {
            if (!isClosed()) {
                log.fatal(x).append(name).append(" ClosedSelectorException in select(): ")
                    .append(x.getMessage()).endl();
                System.exit(1);
            }
        }
    }

    private void spinSelect(long times) {
        try {
            if (log.isDebugEnabled()) {
                log.debug().append(name).append(" calling spinSelect(").append(times).append(")")
                    .endl();
            }

            mark(workDuration);

            while (selector.selectNow() == 0 && !spinWakeSelector && (times-- > 0)) {
            }

            mark(selectDuration);
        } catch (IOException x) {
            if (java.util.regex.Pattern.matches(".*Operation not permitted.*", x.toString())) {
                // There is a documented bug
                // (http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6481709) in some
                // versions of the epoll selector which causes occasional "Operation not permitted"
                // errors to be
                // thrown.
                log.warn().append(name).append(
                    " Ignoring 'Operation not permitted' exception, see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6481709")
                    .endl();
            } else {
                if (!isClosed()) {
                    log.fatal(x).append(name).append(" Unexpected IOException in spinSelect(): ")
                        .append(x.getMessage()).endl();
                    System.exit(1);
                }
            }
        } catch (ClosedSelectorException x) {
            if (!isClosed()) {
                log.fatal(x).append(name).append(" ClosedSelectorException in spinSelect(): ")
                    .append(x.getMessage()).endl();
                System.exit(1);
            }
        }
    }

    /**
     * Gather up selected and timed-out jobs
     *
     * NOTE: assumes that stateLock is held
     */
    private void gather(long now) {
        JobState state;
        int numInvokes = 0;
        // first gather all of the invokable jobs
        for (SelectionKey key : selector.selectedKeys()) {
            ++numInvokes;
            try {
                if ((state = (JobState) key.attachment()) == null) {
                    // clear interest ops, so we don't select in a tight loop
                    if (key.isValid() && key.interestOps() != 0) {
                        if (log.isDebugEnabled()) {
                            log.debug().append(name).append(" clearing interest in orphaned key ")
                                .append(key.toString()).append(" in YASchedulerImpl.gather").endl();
                        }
                        if (key.isValid()) {
                            key.interestOps(0);
                        }
                        keyOrphans.sample(1);
                    }
                } else {
                    key.attach(null);
                    state.readyChannel = key.channel();
                    state.readyOps = key.readyOps();
                    state.gathered = true;
                    state.gatheredNanos = lastNanos;
                    dispatchQueue.add(state);
                    timeoutQueue.remove(state);
                    if (log.isDebugEnabled()) {
                        log.debug().append(name).append(" gather ").append(key.toString())
                            .append(" -> ").append(state.job)
                            .append(", ops=").append(key.readyOps())
                            .append(", ki=").append(key.interestOps())
                            .append(", dq=").append(dispatchQueue.size())
                            .endl();
                    }
                }
            } catch (CancelledKeyException x) {
                // We can't guarantee that some thread won't try to write to the channel and
                // cause it to cancel the key -- if that happens, then we'll get the exception
                // here. But that's okay, because it's either an orphan channel which we just
                // want to get rid of, or the IOJob will get the exception later and handle it.
            }
        }
        selector.selectedKeys().clear();
        invokeCount.sample(numInvokes);

        // now get all of the expired timeouts
        int numTimeouts = 0;
        while (!timeoutQueue.isEmpty() && now >= (state = timeoutQueue.top()).deadline) {
            ++numTimeouts;
            timeoutQueue.removeTop();
            state.gathered = true;
            state.gatheredNanos = lastNanos;
            dispatchQueue.add(state);
        }
        timeoutCount.sample(numTimeouts);

        if (log.isDebugEnabled()) {
            log.debug().append(name).append(" gathered ").append(numInvokes).append(" for I/O and ")
                .append(numTimeouts).append(" timeouts").endl();
        }
    }

    /**
     * dispatch a gathered job, if there are any
     */
    private boolean dispatch(Procedure.Nullary handoff) {
        JobState state;
        SelectableChannel readyChannel;
        int readyOps;
        boolean cancelled;
        synchronized (stateLock) {
            if ((state = dispatchQueue.poll()) == null) {
                return false;
            }

            readyChannel = state.readyChannel;
            readyOps = state.readyOps;
            cancelled = state.cancelled;
            state.readyChannel = null;
            state.readyOps = 0;
            state.gathered = false;
            // NOTE: we only need to record the state as changed if it has a channel;
            // cancelled and timed-out states will just be forgotten.
            if (!cancelled && readyChannel != null) {
                changedState(state);
            }
            if (log.isDebugEnabled()) {
                log.debug().append(name).append(" dispatch ").append(state.job)
                    .append(", ops=").append(readyOps)
                    .append(", dq=").append(dispatchQueue.size())
                    .endl();
            }
            assert readyChannel == null || readyOps != 0;
        }

        // dispatch the job outside of the state lock
        try {
            if (cancelled) {
                if (log.isDebugEnabled()) {
                    log.debug().append(name).append(" cancelled ").append(state.job).endl();
                }
                state.job.cancelled();
            } else {
                if (doTimingStats)
                    gatheredDuration.sample((System.nanoTime() - state.gatheredNanos + 500) / 1000);
                if (readyOps != 0) {
                    if (log.isDebugEnabled()) {
                        log.debug().append(name).append(" invoke ").append(state.job).endl();
                    }
                    state.job.invoke(readyChannel, readyOps, handoff);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug().append(name).append(" timedOut ").append(state.job).endl();
                    }
                    if (handoff != null) {
                        handoff.call();
                    }
                    state.job.timedOut();
                }
            }
        } catch (Throwable x) {
            log.fatal(x).append(": unhandled Throwable in dispatch on job [").append(state.job)
                .append("]: ").append(x.getMessage()).endl();
            throw new RuntimeException(x);
        }

        return true;
    }

    /**
     * Wake up the selector, if necessary.
     *
     * NOTE: assumes that stateLock is held!
     */
    private void maybeWakeSelector() {
        if (selectingTill > 0) {
            if (log.isDebugEnabled()) {
                log.debug().append(name).append(" waking up the scheduler").endl();
            }
            selector.wakeup();
            selectorWakeups.sample(1);
        }

        if (doSpinSelect) {
            spinWakeSelector = true;
        }
    }

    /**
     * Wait for jobs to become ready, then invoke() them all. This method will form the core of the
     * main loop of a scheduler-driven application. The method first waits until:
     *
     * -- the given timeout expires, -- the earliest job-specific timeout expires, or -- one or more
     * jobs becomes ready
     *
     * Note that this method is not synchronized. The application must ensure that it is never
     * called concurrently by more than one thread.
     *
     * @return true, if some work was done.
     */
    public boolean work(long timeout, Procedure.Nullary handoff) {
        if (doSpinSelect) {
            // just use the millis timeout as the number of times to spin
            long times = timeout;
            return spinWork(times, handoff);
        }

        boolean didOne = dispatch(handoff);
        if (!didOne) {
            // apply any changes to the states
            synchronized (stateLock) {
                update();
                long now = currentTimeMillis();
                timeout = computeTimeout(now, timeout);
                assert selectingTill == 0 : "no more than one thread should ever call work!";
                if (timeout > 0) {
                    selectingTill = now + timeout;
                }
            }

            // wait for something to happen
            select(timeout);

            // apply changes while we were waiting, then gather up all of the jobs that can be
            // dispatched
            synchronized (stateLock) {
                selectingTill = 0;
                update();
                long now = currentTimeMillis();
                gather(now);
            }

            // and try again
            didOne = dispatch(handoff);
        }
        return didOne;
    }

    private boolean spinWork(long times, Procedure.Nullary handoff) {
        boolean didOne = dispatch(handoff);
        if (!didOne) {
            // apply any changes to the states
            synchronized (stateLock) {
                update();
                if (!dispatchQueue.isEmpty() || spinWakeSelector) {
                    times = 1; // only want to spin on select once since we have stuff to dispatch
                    spinWakeSelector = false;
                }
                assert selectingTill == 0 : "no more than one thread should ever call work!";
            }

            // spin for something to happen
            spinSelect(times);

            // apply changes while we were waiting, then gather up all of the jobs that can be
            // dispatched
            synchronized (stateLock) {
                selectingTill = 0;
                update();
                long now = currentTimeMillis();
                gather(now);
            }

            // and try again
            didOne = dispatch(handoff);
        }
        return didOne;
    }

    /**
     * Shuts down the scheduler, calling close() on the underlying Selector instance.
     */
    public void close() {
        isClosed = true;
        clear();
        try {
            selector.close();
        } catch (IOException x) {
            log.warn(x).append(name)
                .append(" Scheduler.close: ignoring exception from selector.close(): ")
                .append(x.getMessage()).endl();
        }
    }

    /**
     * Return true if the scheduler is closed, or in the process of closing.
     */
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Clear out the scheduler state
     */
    private void clear() {
        Set<Job> allJobs = getAllJobs();
        for (Job j : allJobs) {
            cancelJob(j);
        }
        log.info().append(name).append(" Scheduler.clear: starting with ").append(allJobs.size())
            .append(" jobs").endl();
        synchronized (stateLock) {
            update();
        }
        ArrayList<SelectionKey> allKeys = getAllKeys();
        for (SelectionKey k : allKeys) {
            k.cancel();
        }
        synchronized (stateLock) {
            update();
        }
        try {
            selector.selectNow();
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        while (true) {
            try {
                if (!dispatch(null)) {
                    break;
                }
            } catch (Exception x) {
                log.warn().append(name).append(" Scheduler.clear: ignoring shutdown exception: ")
                    .append(x).endl();
            }
        }
        log.info().append(name).append(" Scheduler.clear: finished").endl();
    }

    /**
     * return the set of all jobs known to the scheduler, in whatever state
     */
    private Set<Job> getAllJobs() {
        synchronized (stateLock) {
            update();
            Set<Job> result = new HashSet<Job>();
            timeoutQueue.junitGetAllJobs(result);
            for (JobState state : changedStates) {
                assert state != null;
                if (state.job != null) {
                    result.add(state.job);
                }
            }
            for (SelectionKey key : junitGetAllKeys()) {
                Object attachment;
                if (key != null && (attachment = key.attachment()) != null
                    && attachment instanceof JobState) {
                    JobState state = (JobState) attachment;
                    if (state.job != null) {
                        result.add(state.job);
                    }
                }
            }
            return result;
        }
    }

    /**
     * Return the selection keys currently known to the scheduler.
     */
    private ArrayList<SelectionKey> getAllKeys() {
        synchronized (stateLock) {
            update();
            Set<SelectionKey> keys = selector.keys();
            selector.wakeup();
            synchronized (keys) {
                return new ArrayList<SelectionKey>(keys);
            }
        }
    }

    // --------------------------------------------------------------------------
    // test support methods (white-box)
    // --------------------------------------------------------------------------

    public Selector junitGetSelector() {
        return selector;
    }

    /**
     * return the set of all jobs known to the scheduler, in whatever state
     */
    public Set<Job> junitGetAllJobs() {
        return getAllJobs();
    }

    /**
     * Return the contents of the timeout queue, in deadline order
     * 
     * @return the jobs in the timeout queue
     */
    public ArrayList<Job> junitGetTimeoutQueue() {
        synchronized (stateLock) {
            update();
            ArrayList<Job> result = new ArrayList<Job>(timeoutQueue.size());
            try {
                JobStateTimeoutQueue q = (JobStateTimeoutQueue) timeoutQueue.clone();
                while (!q.isEmpty()) {
                    result.add(q.top().job);
                    q.removeTop();
                }
            } catch (CloneNotSupportedException x) {
                // ignore
            }
            return result;
        }
    }

    /**
     * Return the selection keys currently known to the scheduler.
     */
    public ArrayList<SelectionKey> junitGetAllKeys() {
        return getAllKeys();
    }

    /**
     * Return the selection keys currently known to the scheduler.
     */
    public ArrayList<SelectionKey> junitGetReadyKeys() {
        return new ArrayList<SelectionKey>(selector.selectedKeys());
    }

    /**
     * Return a map containing all channels and the jobs to which they are associated.
     */
    public Map<SelectableChannel, Job> junitGetChannelsAndJobs() {
        synchronized (stateLock) {
            update();
            Map<SelectableChannel, Job> result = new HashMap<SelectableChannel, Job>();
            for (SelectionKey key : junitGetAllKeys()) {
                Object attachment;
                if (key != null && (attachment = key.attachment()) != null
                    && attachment instanceof JobState) {
                    JobState state = (JobState) attachment;
                    if (state.job != null) {
                        result.put(key.channel(), ((JobState) attachment).job);
                    }
                }
            }
            return result;
        }
    }

    /**
     * Return true if the timeout queue invariant holds.
     */
    public boolean junitTestTimeoutQueueInvariant() {
        synchronized (stateLock) {
            return timeoutQueue.testInvariant("in call from junit");
        }
    }
}
