/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.db.v2.utils.RuntimeMemory;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.profiling.ThreadProfiler;

import java.io.Serializable;

import static io.deephaven.db.tables.lang.DBLanguageFunctionUtil.minus;
import static io.deephaven.util.QueryConstants.*;

/**
 * Per-operation instrumentation node for hierarchical performance recording. Note that this class has an unusually
 * intimate relationship with another class, {@link QueryPerformanceRecorder}. Changes to either should take this lack
 * of encapsulation into account.
 */
public class QueryPerformanceNugget implements Serializable, AutoCloseable {
    private static final QueryPerformanceLogThreshold LOG_THRESHOLD = new QueryPerformanceLogThreshold("", 1_000_000);
    private static final QueryPerformanceLogThreshold UNINSTRUMENTED_LOG_THRESHOLD =
            new QueryPerformanceLogThreshold("Uninstrumented", 1_000_000_000);
    private static final int MAX_DESCRIPTION_LENGTH = 16 << 10;

    private static final long serialVersionUID = 2L;

    /**
     * A re-usable "dummy" nugget which will never collect any information or be recorded.
     */
    static final QueryPerformanceNugget DUMMY_NUGGET = new QueryPerformanceNugget();

    private final int evaluationNumber;
    private final int depth;
    private final String description;
    private final boolean isUser;
    private final String callerLine;
    private final long inputSize;

    private final long startClockTime;

    private final long startTimeNanos;
    private final long startCpuNanos;
    private final long startUserCpuNanos;
    private final long startFreeMemory;
    private final long startTotalMemory;
    private final long startAllocatedBytes;
    private final long startPoolAllocatedBytes;
    private volatile QueryState state;

    private Long totalTimeNanos;
    private long diffCpuNanos;
    private long diffUserCpuNanos;
    private long totalFreeMemory;
    private long totalUsedMemory;
    private long diffFreeMemory;
    private long diffTotalMemory;
    private long diffAllocatedBytes;
    private long diffPoolAllocatedBytes;

    private boolean shouldLogMeAndStackParents;

    /**
     * Constructor for query-level nuggets.
     *
     * @param evaluationNumber A unique identifier for the query evaluation that triggered this nugget creation
     * @param description The operation description
     */
    QueryPerformanceNugget(final int evaluationNumber, final String description) {
        this(evaluationNumber, NULL_INT, description, false, NULL_LONG);
    }

    /**
     * Full constructor for nuggets.
     *
     * @param evaluationNumber A unique identifier for the query evaluation that triggered this nugget creation
     * @param depth Depth in the evaluation chain for the respective operation
     * @param description The operation description
     * @param isUser Whether this is a "user" nugget or one created by the system
     * @param inputSize The size of the input data
     */
    QueryPerformanceNugget(final int evaluationNumber, final int depth,
            final String description, final boolean isUser, final long inputSize) {
        this.evaluationNumber = evaluationNumber;
        this.depth = depth;
        if (description.length() > MAX_DESCRIPTION_LENGTH) {
            this.description = description.substring(0, MAX_DESCRIPTION_LENGTH) + " ... [truncated "
                    + (description.length() - MAX_DESCRIPTION_LENGTH) + " bytes]";
        } else {
            this.description = description;
        }
        this.isUser = isUser;
        this.inputSize = inputSize;

        final RuntimeMemory runtimeMemory = RuntimeMemory.getInstance();
        startFreeMemory = runtimeMemory.freeMemory();
        startTotalMemory = runtimeMemory.totalMemory();

        startAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
        startPoolAllocatedBytes = QueryPerformanceRecorder.getPoolAllocatedBytesForCurrentThread();

        callerLine = QueryPerformanceRecorder.getCallerLine();

        startClockTime = System.currentTimeMillis();
        startTimeNanos = System.nanoTime();

        startCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadCpuTime();
        startUserCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadUserTime();

        state = QueryState.RUNNING;
        shouldLogMeAndStackParents = false;
    }

    /**
     * Construct a "dummy" nugget, which will never gather any information or be recorded.
     */
    private QueryPerformanceNugget() {
        evaluationNumber = NULL_INT;
        depth = 0;
        description = null;
        isUser = false;
        inputSize = NULL_LONG;

        startFreeMemory = NULL_LONG;
        startTotalMemory = NULL_LONG;

        startAllocatedBytes = NULL_LONG;
        startPoolAllocatedBytes = NULL_LONG;

        callerLine = null;

        startClockTime = NULL_LONG;
        startTimeNanos = NULL_LONG;

        startCpuNanos = NULL_LONG;
        startUserCpuNanos = NULL_LONG;

        state = null; // This turns close into a no-op.
        shouldLogMeAndStackParents = false;
    }

    public void done() {
        done(QueryPerformanceRecorder.getInstance());
    }

    /**
     * Mark this nugget {@link QueryState#FINISHED} and notify the recorder.
     *
     * @param recorder The recorder to notify
     * @return if the nugget passes logging thresholds.
     */
    public boolean done(QueryPerformanceRecorder recorder) {
        return close(QueryState.FINISHED, recorder);
    }

    /**
     * AutoCloseable implementation - wraps the no-argument version of done() used by query code outside of the
     * QueryPerformance(Recorder/Nugget), reporting successful completion to the thread-local QueryPerformanceRecorder
     * instance.
     */
    @Override
    public void close() {
        done();
    }

    @SuppressWarnings("WeakerAccess")
    public boolean abort(QueryPerformanceRecorder recorder) {
        return close(QueryState.INTERRUPTED, recorder);
    }

    /**
     * Finish the nugget and record the current state of the world.
     *
     * @param closingState The current query state. If it is anything other than {@link QueryState#RUNNING} nothing will
     *        happen and it will return false;
     *
     * @param recorderToNotify The {@link QueryPerformanceRecorder} to notify this nugget is closing.
     * @return If the nugget passes criteria for logging.
     */
    private boolean close(QueryState closingState, QueryPerformanceRecorder recorderToNotify) {
        final long currentThreadUserTime = ThreadProfiler.DEFAULT.getCurrentThreadUserTime();
        final long currentThreadCpuTime = ThreadProfiler.DEFAULT.getCurrentThreadCpuTime();
        if (state != QueryState.RUNNING) {
            return false;
        }

        synchronized (this) {
            if (state != QueryState.RUNNING) {
                return false;
            }

            diffUserCpuNanos = minus(currentThreadUserTime, startUserCpuNanos);
            diffCpuNanos = minus(currentThreadCpuTime, startCpuNanos);

            totalTimeNanos = System.nanoTime() - startTimeNanos;

            final RuntimeMemory runtimeMemory = RuntimeMemory.getInstance();
            totalFreeMemory = runtimeMemory.freeMemory();
            totalUsedMemory = runtimeMemory.totalMemory();
            diffFreeMemory = totalFreeMemory - startFreeMemory;
            diffTotalMemory = totalUsedMemory - startTotalMemory;

            diffPoolAllocatedBytes =
                    minus(QueryPerformanceRecorder.getPoolAllocatedBytesForCurrentThread(), startPoolAllocatedBytes);
            diffAllocatedBytes = minus(ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes(), startAllocatedBytes);

            state = closingState;
            return recorderToNotify.releaseNugget(this);
        }
    }

    @Override
    public String toString() {
        return Integer.toString(evaluationNumber)
                + ":" + description
                + ":" + callerLine;
    }

    public int getEvaluationNumber() {
        return evaluationNumber;
    }

    public int getDepth() {
        return depth;
    }

    public String getName() {
        return description;
    }

    public boolean isUser() {
        return isUser;
    }

    public boolean isTopLevel() {
        return depth == 0;
    }

    public long getInputSize() {
        return inputSize;
    }

    public String getCallerLine() {
        return callerLine;
    }

    /**
     * @return nanoseconds elapsed, once state != QueryState.RUNNING() has been called.
     */
    public Long getTotalTimeNanos() {
        return totalTimeNanos;
    }

    /**
     * @return wall clock time in milliseconds from the epoch
     */
    public long getStartClockTime() {
        return startClockTime;
    }

    /**
     * Get nanoseconds of CPU time attributed to the instrumented operation.
     *
     * @return The nanoseconds of CPU time attributed to the instrumented operation, or {@link QueryConstants#NULL_LONG}
     *         if not enabled/supported.
     */
    public long getCpuNanos() {
        return diffCpuNanos;
    }

    /**
     * Get nanoseconds of user mode CPU time attributed to the instrumented operation.
     *
     * @return The nanoseconds of user mode CPU time attributed to the instrumented operation, or
     *         {@link QueryConstants#NULL_LONG} if not enabled/supported.
     */
    public long getUserCpuNanos() {
        return diffUserCpuNanos;
    }

    /**
     * @return free memory at completion
     */
    public long getEndFreeMemory() {
        return totalFreeMemory;
    }

    /**
     * @return total memory used at completion
     */
    public long getEndTotalMemory() {
        return totalUsedMemory;
    }

    /**
     * @return free memory difference between time of completion and creation
     */
    public long getDiffFreeMemory() {
        return diffFreeMemory;
    }

    /**
     * @return total (allocated high water mark) memory difference between time of completion and creation
     */
    public long getDiffTotalMemory() {
        return diffTotalMemory;
    }

    /**
     * Get bytes of allocated memory attributed to the instrumented operation.
     *
     * @return The bytes of allocated memory attributed to the instrumented operation, or
     *         {@link QueryConstants#NULL_LONG} if not enabled/supported.
     */
    public long getAllocatedBytes() {
        return diffAllocatedBytes;
    }

    /**
     * Get bytes of allocated pooled/reusable memory attributed to the instrumented operation.
     *
     * @return The bytes of allocated pooled/reusable memory attributed to the instrumented operation, or
     *         {@link QueryConstants#NULL_LONG} if not enabled/supported.
     */
    public long getPoolAllocatedBytes() {
        return diffPoolAllocatedBytes;
    }

    /**
     * @return true if this nugget was interrupted by an abort() call.
     */
    public boolean wasInterrupted() {
        return state == QueryState.INTERRUPTED;
    }

    /**
     * Ensure this nugget gets logged, alongside its stack of nesting operations.
     */
    public void setShouldLogMeAndStackParents() {
        shouldLogMeAndStackParents = true;
    }

    /**
     * @return true if this nugget triggers the logging of itself and every other nugget in its stack of nesting
     *         operations.
     */
    public boolean shouldLogMenAndStackParents() {
        return shouldLogMeAndStackParents;
    }

    /**
     * Suppress de minimus performance nuggets using the properties defined above.
     *
     * @param isUninstrumented this nugget for uninstrumented code? If so the thresholds for inclusion in the logs are
     *        configured distinctly.
     *
     * @return if this nugget is significant enough to be logged.
     */
    boolean shouldLogNugget(final boolean isUninstrumented) {
        if (shouldLogMeAndStackParents) {
            return true;
        }
        // Nuggets will have a null value for total time if they weren't closed for a RUNNING query; this is an abnormal
        // condition and the nugget should be logged
        if (getTotalTimeNanos() == null) {
            return true;
        }

        if (isUninstrumented) {
            return UNINSTRUMENTED_LOG_THRESHOLD.shouldLog(getTotalTimeNanos());
        } else {
            return LOG_THRESHOLD.shouldLog(getTotalTimeNanos());
        }
    }
}
