/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

import io.deephaven.auth.AuthContext;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.util.RuntimeMemory;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;

import java.io.Serializable;

import static io.deephaven.util.QueryConstants.*;

/**
 * Per-operation instrumentation node for hierarchical performance recording. Note that this class has an unusually
 * intimate relationship with another class, {@link QueryPerformanceRecorder}. Changes to either should take this lack
 * of encapsulation into account.
 */
public class QueryPerformanceNugget extends BasePerformanceEntry implements Serializable, SafeCloseable {
    private static final QueryPerformanceLogThreshold LOG_THRESHOLD = new QueryPerformanceLogThreshold("", 1_000_000);
    private static final QueryPerformanceLogThreshold UNINSTRUMENTED_LOG_THRESHOLD =
            new QueryPerformanceLogThreshold("Uninstrumented", 1_000_000_000);
    private static final int MAX_DESCRIPTION_LENGTH = 16 << 10;

    private static final long serialVersionUID = 2L;

    /**
     * A re-usable "dummy" nugget which will never collect any information or be recorded.
     */
    static final QueryPerformanceNugget DUMMY_NUGGET = new QueryPerformanceNugget();

    private final long evaluationNumber;
    private final long parentEvaluationNumber;
    private final int operationNumber;
    private final int parentOperationNumber;
    private final int depth;
    private final String description;
    private final boolean isUser;
    private final boolean isQueryLevel;
    private final long inputSize;

    private final AuthContext authContext;
    private final String callerLine;

    private final long startClockTime;
    private long endClockTime;

    private volatile QueryState state;


    private final RuntimeMemory.Sample startMemorySample;
    private final RuntimeMemory.Sample endMemorySample;

    private boolean shouldLogMeAndStackParents;

    /**
     * For threaded operations we want to accumulate the CPU time, allocations, and read operations to the enclosing
     * nugget of the main operation. For the initialization we ignore the wall clock time taken in the thread pool.
     */
    private BasePerformanceEntry basePerformanceEntry;

    /**
     * Constructor for query-level nuggets.
     *
     * @param evaluationNumber A unique identifier for the query evaluation that triggered this nugget creation
     * @param description The operation description
     */
    QueryPerformanceNugget(final long evaluationNumber, final long parentEvaluationNumber, final String description) {
        this(evaluationNumber, parentEvaluationNumber, NULL_INT, NULL_INT, NULL_INT, description, false, true,
                NULL_LONG);
    }

    /**
     * Full constructor for nuggets.
     *
     * @param evaluationNumber A unique identifier for the query evaluation that triggered this nugget creation
     * @param parentEvaluationNumber The unique identifier of the parent evaluation or {@link QueryConstants#NULL_LONG}
     *        if none
     * @param operationNumber A unique identifier for the operation within a query evaluation
     * @param parentOperationNumber The unique identifier of the parent operation or {@link QueryConstants#NULL_INT} if
     *        none
     * @param depth Depth in the evaluation chain for the respective operation
     * @param description The operation description
     * @param isUser Whether this is a "user" nugget or one created by the system
     * @param inputSize The size of the input data
     */
    QueryPerformanceNugget(
            final long evaluationNumber,
            final long parentEvaluationNumber,
            final int operationNumber,
            final int parentOperationNumber,
            final int depth,
            final String description,
            final boolean isUser,
            final boolean isQueryLevel,
            final long inputSize) {
        startMemorySample = new RuntimeMemory.Sample();
        endMemorySample = new RuntimeMemory.Sample();
        this.evaluationNumber = evaluationNumber;
        this.parentEvaluationNumber = parentEvaluationNumber;
        this.operationNumber = operationNumber;
        this.parentOperationNumber = parentOperationNumber;
        this.depth = depth;
        if (description.length() > MAX_DESCRIPTION_LENGTH) {
            this.description = description.substring(0, MAX_DESCRIPTION_LENGTH) + " ... [truncated "
                    + (description.length() - MAX_DESCRIPTION_LENGTH) + " bytes]";
        } else {
            this.description = description;
        }
        this.isUser = isUser;
        this.isQueryLevel = isQueryLevel;
        this.inputSize = inputSize;

        authContext = ExecutionContext.getContext().getAuthContext();
        callerLine = QueryPerformanceRecorder.getCallerLine();

        final RuntimeMemory runtimeMemory = RuntimeMemory.getInstance();
        runtimeMemory.read(startMemorySample);

        startClockTime = System.currentTimeMillis();
        onBaseEntryStart();

        state = QueryState.RUNNING;
        shouldLogMeAndStackParents = false;
    }

    /**
     * Construct a "dummy" nugget, which will never gather any information or be recorded.
     */
    private QueryPerformanceNugget() {
        startMemorySample = null;
        endMemorySample = null;
        evaluationNumber = NULL_LONG;
        parentEvaluationNumber = NULL_LONG;
        operationNumber = NULL_INT;
        parentOperationNumber = NULL_INT;
        depth = 0;
        description = null;
        isUser = false;
        isQueryLevel = false;
        inputSize = NULL_LONG;

        authContext = null;
        callerLine = null;

        startClockTime = NULL_LONG;

        basePerformanceEntry = null;

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
    public boolean done(final QueryPerformanceRecorder recorder) {
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
    public boolean abort(final QueryPerformanceRecorder recorder) {
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
    private boolean close(final QueryState closingState, final QueryPerformanceRecorder recorderToNotify) {
        if (state != QueryState.RUNNING) {
            return false;
        }

        synchronized (this) {
            if (state != QueryState.RUNNING) {
                return false;
            }

            endClockTime = System.currentTimeMillis();
            onBaseEntryEnd();

            final RuntimeMemory runtimeMemory = RuntimeMemory.getInstance();
            runtimeMemory.read(endMemorySample);

            if (basePerformanceEntry != null) {
                accumulate(basePerformanceEntry);
            }

            state = closingState;
            return recorderToNotify.releaseNugget(this);
        }
    }

    @Override
    public String toString() {
        return evaluationNumber
                + ":" + operationNumber
                + ":" + description
                + ":" + callerLine;
    }

    public long getEvaluationNumber() {
        return evaluationNumber;
    }

    public long getParentEvaluationNumber() {
        return parentEvaluationNumber;
    }

    public int getOperationNumber() {
        return operationNumber;
    }

    public int getParentOperationNumber() {
        return parentOperationNumber;
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

    public boolean isQueryLevel() {
        return isQueryLevel;
    }

    public boolean isTopLevel() {
        return depth == 0;
    }

    public long getInputSize() {
        return inputSize;
    }

    /**
     * @return The {@link AuthContext} that was installed when this QueryPerformanceNugget was constructed
     */
    public AuthContext getAuthContext() {
        return authContext;
    }

    public String getCallerLine() {
        return callerLine;
    }

    /**
     * @return nanoseconds elapsed, once state != QueryState.RUNNING() has been called.
     */
    public long getTotalTimeNanos() {
        return getIntervalUsageNanos();
    }

    /**
     * @return wall clock start time in nanoseconds from the epoch
     */
    public long getStartClockTime() {
        return DateTimeUtils.millisToNanos(startClockTime);
    }

    /**
     * @return wall clock end time in nanoseconds from the epoch
     */
    public long getEndClockTime() {
        return DateTimeUtils.millisToNanos(endClockTime);
    }


    /**
     * Get nanoseconds of CPU time attributed to the instrumented operation.
     *
     * @return The nanoseconds of CPU time attributed to the instrumented operation, or {@link QueryConstants#NULL_LONG}
     *         if not enabled/supported.
     */
    public long getCpuNanos() {
        return getIntervalCpuNanos();
    }

    /**
     * Get nanoseconds of user mode CPU time attributed to the instrumented operation.
     *
     * @return The nanoseconds of user mode CPU time attributed to the instrumented operation, or
     *         {@link QueryConstants#NULL_LONG} if not enabled/supported.
     */
    public long getUserCpuNanos() {
        return getIntervalUserCpuNanos();
    }

    /**
     * @return free memory at completion
     */
    public long getEndFreeMemory() {
        return endMemorySample.freeMemory;
    }

    /**
     * @return total memory used at completion
     */
    public long getEndTotalMemory() {
        return endMemorySample.totalMemory;
    }

    /**
     * @return free memory difference between time of completion and creation
     */
    public long getDiffFreeMemory() {
        return endMemorySample.freeMemory - startMemorySample.freeMemory;
    }

    /**
     * @return total (allocated high water mark) memory difference between time of completion and creation
     */
    public long getDiffTotalMemory() {
        return endMemorySample.totalMemory - startMemorySample.totalMemory;
    }

    /**
     * @return Number of garbage collections between time of completion and creation
     */
    public long getDiffCollections() {
        return endMemorySample.totalCollections - startMemorySample.totalCollections;
    }

    /**
     * @return Time spent in garbage collection, in milliseconds, between time of completion and creation
     */
    public long getDiffCollectionTimeNanos() {
        return DateTimeUtils
                .millisToNanos(endMemorySample.totalCollectionTimeMs - startMemorySample.totalCollectionTimeMs);
    }

    /**
     * Get bytes of allocated memory attributed to the instrumented operation.
     *
     * @return The bytes of allocated memory attributed to the instrumented operation, or
     *         {@link QueryConstants#NULL_LONG} if not enabled/supported.
     */
    public long getAllocatedBytes() {
        return getIntervalAllocatedBytes();
    }

    /**
     * Get bytes of allocated pooled/reusable memory attributed to the instrumented operation.
     *
     * @return The bytes of allocated pooled/reusable memory attributed to the instrumented operation, or
     *         {@link QueryConstants#NULL_LONG} if not enabled/supported.
     */
    public long getPoolAllocatedBytes() {
        return getIntervalPoolAllocatedBytes();
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
    public boolean shouldLogMeAndStackParents() {
        return shouldLogMeAndStackParents;
    }

    /**
     * When we track data from other threads that should be attributed to this operation, we tack extra
     * BasePerformanceEntry values onto this nugget when it is closed.
     * <p>
     * The CPU time, reads, and allocations are counted against this nugget. Wall clock time is ignored.
     */
    public synchronized void addBaseEntry(BasePerformanceEntry baseEntry) {
        if (this.basePerformanceEntry == null) {
            this.basePerformanceEntry = baseEntry;
        } else {
            this.basePerformanceEntry.accumulate(baseEntry);
        }
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

        if (isUninstrumented) {
            return UNINSTRUMENTED_LOG_THRESHOLD.shouldLog(getTotalTimeNanos());
        } else {
            return LOG_THRESHOLD.shouldLog(getTotalTimeNanos());
        }
    }
}
