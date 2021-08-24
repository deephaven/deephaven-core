/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.sched;

import java.nio.channels.SelectableChannel;

/**
 * The per-scheduler state for a job. Note that this class is package-private.
 */
class JobState implements Cloneable {
    /** the job */
    final Job job;

    /** the update count for this job state */
    long updateClock = 0;

    /** the current deadline for this job */
    long deadline = Long.MAX_VALUE;

    /** the job's current position in the scheduler's timeout queue */
    int tqPos = 0;

    /** true, if this job has been invoked or has timed out */
    boolean gathered = false;

    /** true if the job has been forgotten after being dispatched and not reinstalled */
    boolean forgotten = false;

    /** true if this job has been explicitly cancelled */
    boolean cancelled = false;

    /** this is the channel we are waiting on in the selector */
    SelectableChannel waitChannel = null;

    /** the channel on which the job is ready to be dispatched, or null */
    SelectableChannel readyChannel = null;

    /** the operation set on which the job is ready to be dispatched, or zero */
    int readyOps = 0;

    /** the channel on which this job will select in the next scheduler loop */
    SelectableChannel nextChannel = null;

    /** the interest set on which this job will select in the next scheduler loop */
    int nextOps = 0;

    /** the timeout deadline of this job in the next scheduler loop */
    long nextDeadline = Long.MAX_VALUE;

    /** the nano-time when this job was last enqueued */
    long gatheredNanos = 0;

    /** constructor stores the back-link to the job */
    JobState(Job job) {
        this.job = job;
    }

    /**
     * Clone this object
     */
    public JobState clone() throws CloneNotSupportedException {
        return (JobState) super.clone();
    }
}
