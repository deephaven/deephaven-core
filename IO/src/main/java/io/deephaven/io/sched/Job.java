/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.sched;

import io.deephaven.base.Procedure;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;

import java.nio.channels.SelectableChannel;
import java.io.IOException;

/**
 * This is the base class for jobs that can be invoked by the scheduler.
 */
public abstract class Job implements LogOutputAppendable {

    // --------------------------------------------------------------------------
    // public interface
    // --------------------------------------------------------------------------

    /**
     * This method is invoked by the scheduler when the job's channel becomes ready.
     *
     * @param channel the channel which has become ready
     * @param readyOps the operations which can be performed on this channel without blocking
     * @returns the modified readyOps after the invocation; if non-zero, the job will be invoked
     *          again with these
     * @throws IOException - if something bad happens
     */
    public abstract int invoke(SelectableChannel channel, int readyOps, Procedure.Nullary handoff)
        throws IOException;

    /**
     * This method is invoked if the job times out.
     */
    public abstract void timedOut();

    /**
     * This method is called if the job is explicitly cancelled before it becomes ready or times
     * out.
     */
    public abstract void cancelled();

    // --------------------------------------------------------------------------
    // scheduler state management
    // --------------------------------------------------------------------------

    // TODO: currently, we assume that the scheduler is a singleton, or at the least
    // TODO: that no job will be used with more than one scheduler throughout its lifetime.
    // TODO: If this changes, we will have to change the state pointer to a set.

    /** the link to the scheduler's state for this job */
    JobState state;

    /** return the state for the given scheduler, or null */
    final JobState getStateFor(Scheduler sched) {
        return state;
    }

    /** return or create the state for the given scheduler */
    final JobState makeStateFor(Scheduler sched) {
        return state == null ? (state = new JobState(this)) : state;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(LogOutput.BASIC_FORMATTER, this);
    }
}
