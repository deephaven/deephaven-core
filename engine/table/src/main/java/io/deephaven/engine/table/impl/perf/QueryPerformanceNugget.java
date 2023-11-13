/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

import io.deephaven.auth.AuthContext;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.util.RuntimeMemory;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.function.Predicate;

import static io.deephaven.util.QueryConstants.*;

/**
 * Per-operation instrumentation node for hierarchical performance recording. Note that this class has an unusually
 * intimate relationship with another class, {@link QueryPerformanceRecorder}. Changes to either should take this lack
 * of encapsulation into account.
 */
public class QueryPerformanceNugget extends BasePerformanceEntry implements SafeCloseable {
    private static final QueryPerformanceLogThreshold LOG_THRESHOLD = new QueryPerformanceLogThreshold("", 1_000_000);
    private static final QueryPerformanceLogThreshold UNINSTRUMENTED_LOG_THRESHOLD =
            new QueryPerformanceLogThreshold("Uninstrumented", 1_000_000_000);
    private static final int MAX_DESCRIPTION_LENGTH = 16 << 10;

    /**
     * A re-usable "dummy" nugget which will never collect any information or be recorded.
     */
    static final QueryPerformanceNugget DUMMY_NUGGET = new QueryPerformanceNugget() {
        @Override
        public void accumulate(@NotNull BasePerformanceEntry entry) {
            // non-synchronized no-op override
        }

        @Override
        public boolean shouldLogThisAndStackParents() {
            return false;
        }

        @Override
        boolean shouldLogNugget(boolean isUninstrumented) {
            return false;
        }
    };

    public interface Factory {
        /**
         * Factory method for query-level nuggets.
         *
         * @param evaluationNumber A unique identifier for the query evaluation that triggered this nugget creation
         * @param description The operation description
         * @param onCloseCallback A callback that is invoked when the nugget is closed. It returns whether the nugget
         *        should be logged.
         * @return A new nugget
         */
        default QueryPerformanceNugget createForQuery(
                final long evaluationNumber,
                @NotNull final String description,
                @NotNull final Predicate<QueryPerformanceNugget> onCloseCallback) {
            return new QueryPerformanceNugget(evaluationNumber, NULL_LONG, NULL_INT, NULL_INT, NULL_INT,
                    description, false, NULL_LONG, onCloseCallback);
        }

        /**
         * Factory method for sub-query-level nuggets.
         *
         * @param parentQuery The parent query nugget
         * @param evaluationNumber A unique identifier for the sub-query evaluation that triggered this nugget creation
         * @param description The operation description
         * @param onCloseCallback A callback that is invoked when the nugget is closed. It returns whether the nugget
         *        should be logged.
         * @return A new nugget
         */
        default QueryPerformanceNugget createForSubQuery(
                @NotNull final QueryPerformanceNugget parentQuery,
                final long evaluationNumber,
                @NotNull final String description,
                @NotNull final Predicate<QueryPerformanceNugget> onCloseCallback) {
            Assert.eqTrue(parentQuery.isQueryLevel(), "parentQuery.isQueryLevel()");
            return new QueryPerformanceNugget(evaluationNumber, parentQuery.getEvaluationNumber(),
                    NULL_INT, NULL_INT, NULL_INT, description, false, NULL_LONG, onCloseCallback);
        }

        /**
         * Factory method for operation-level nuggets.
         *
         * @param parentQueryOrOperation The parent query / operation nugget
         * @param operationNumber A query-unique identifier for the operation
         * @param description The operation description
         * @param onCloseCallback A callback that is invoked when the nugget is closed. It returns whether the nugget
         *        should be logged.
         * @return A new nugget
         */
        default QueryPerformanceNugget createForOperation(
                @NotNull final QueryPerformanceNugget parentQueryOrOperation,
                final int operationNumber,
                final String description,
                final long inputSize,
                @NotNull final Predicate<QueryPerformanceNugget> onCloseCallback) {
            int depth = parentQueryOrOperation.getDepth();
            if (depth == NULL_INT) {
                depth = 0;
            } else {
                ++depth;
            }

            return new QueryPerformanceNugget(
                    parentQueryOrOperation.getEvaluationNumber(),
                    parentQueryOrOperation.getParentEvaluationNumber(),
                    operationNumber,
                    parentQueryOrOperation.getOperationNumber(),
                    depth,
                    description,
                    true, // operations are always user
                    inputSize,
                    onCloseCallback);
        }

        /**
         * Factory method for catch-all nuggets.
         *
         * @param parentQuery The parent query nugget
         * @param operationNumber A query-unique identifier for the operation
         * @param onCloseCallback A callback that is invoked when the nugget is closed. It returns whether the nugget
         *        should be logged.
         * @return A new nugget
         */
        default QueryPerformanceNugget createForCatchAll(
                @NotNull final QueryPerformanceNugget parentQuery,
                final int operationNumber,
                @NotNull final Predicate<QueryPerformanceNugget> onCloseCallback) {
            Assert.eqTrue(parentQuery.isQueryLevel(), "parentQuery.isQueryLevel()");
            return new QueryPerformanceNugget(
                    parentQuery.getEvaluationNumber(),
                    parentQuery.getParentEvaluationNumber(),
                    operationNumber,
                    NULL_INT, // catch all has no parent operation
                    0, // catch all is a root operation
                    QueryPerformanceRecorder.UNINSTRUMENTED_CODE_DESCRIPTION,
                    false, // catch all is not user
                    NULL_LONG,
                    onCloseCallback); // catch all has no input size
        }
    }

    public static final Factory DEFAULT_FACTORY = new Factory() {};

    private final long evaluationNumber;
    private final long parentEvaluationNumber;
    private final int operationNumber;
    private final int parentOperationNumber;
    private final int depth;
    private final String description;
    private final boolean isUser;
    private final long inputSize;
    private final Predicate<QueryPerformanceNugget> onCloseCallback;
    private final AuthContext authContext;
    private final String callerLine;

    private long startClockEpochNanos = NULL_LONG;
    private long endClockEpochNanos = NULL_LONG;

    private volatile QueryState state;


    private final RuntimeMemory.Sample startMemorySample;
    private final RuntimeMemory.Sample endMemorySample;

    private boolean shouldLogThisAndStackParents;

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
     * @param onCloseCallback A callback that is invoked when the nugget is closed. It returns whether the nugget should
     *        be logged.
     */
    protected QueryPerformanceNugget(
            final long evaluationNumber,
            final long parentEvaluationNumber,
            final int operationNumber,
            final int parentOperationNumber,
            final int depth,
            @NotNull final String description,
            final boolean isUser,
            final long inputSize,
            @NotNull final Predicate<QueryPerformanceNugget> onCloseCallback) {
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
        this.inputSize = inputSize;
        this.onCloseCallback = onCloseCallback;

        authContext = ExecutionContext.getContext().getAuthContext();
        callerLine = QueryPerformanceRecorder.getCallerLine();

        final RuntimeMemory runtimeMemory = RuntimeMemory.getInstance();
        runtimeMemory.read(startMemorySample);

        startClockEpochNanos = DateTimeUtils.millisToNanos(System.currentTimeMillis());
        onBaseEntryStart();

        state = QueryState.RUNNING;
        shouldLogThisAndStackParents = false;
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
        inputSize = NULL_LONG;
        onCloseCallback = null;

        authContext = null;
        callerLine = null;

        state = null; // This turns close into a no-op.
        shouldLogThisAndStackParents = false;
    }

    public void markStartTime() {
        if (startClockEpochNanos != NULL_LONG) {
            throw new IllegalStateException("Nugget was already started");
        }

        startClockEpochNanos = DateTimeUtils.millisToNanos(System.currentTimeMillis());
    }

    /**
     * Mark this nugget {@link QueryState#FINISHED} and notify the recorder.
     *
     * @return if the nugget passes logging thresholds.
     */
    public boolean done() {
        return close(QueryState.FINISHED);
    }

    /**
     * Mark this nugget {@link QueryState#FINISHED} and notify the recorder. Is an alias for {@link #done()}.
     * <p>
     * {@link SafeCloseable} implementation for try-with-resources.
     */
    @Override
    public void close() {
        close(QueryState.FINISHED);
    }

    public boolean abort() {
        return close(QueryState.INTERRUPTED);
    }

    /**
     * Finish the nugget and record the current state of the world.
     *
     * @param closingState The current query state. If it is anything other than {@link QueryState#RUNNING} nothing will
     *        happen and it will return false;
     *
     * @return If the nugget passes criteria for logging.
     */
    private boolean close(final QueryState closingState) {
        if (state != QueryState.RUNNING) {
            return false;
        }

        synchronized (this) {
            if (state != QueryState.RUNNING) {
                return false;
            }

            if (startClockEpochNanos == NULL_LONG) {
                throw new IllegalStateException("Nugget was never started");
            }

            onBaseEntryEnd();
            endClockEpochNanos = DateTimeUtils.millisToNanos(System.currentTimeMillis());

            final RuntimeMemory runtimeMemory = RuntimeMemory.getInstance();
            runtimeMemory.read(endMemorySample);

            state = closingState;
            return onCloseCallback.test(this);
        }
    }

    @Override
    public String toString() {
        return evaluationNumber
                + ":" + (isQueryLevel() ? "query_level" : operationNumber)
                + ":" + description
                + ":" + callerLine;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        // override BasePerformanceEntry's impl and match toString()
        return logOutput.append(toString());
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
        return operationNumber == NULL_INT;
    }

    @SuppressWarnings("unused")
    public boolean isTopLevelQuery() {
        return isQueryLevel() && parentEvaluationNumber == NULL_LONG;
    }

    @SuppressWarnings("unused")
    public boolean isTopLevelOperation() {
        // note that query level nuggets have depth == NULL_INT
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
     * @return wall clock start time in nanoseconds from the epoch
     */
    public long getStartClockEpochNanos() {
        return startClockEpochNanos;
    }

    /**
     * @return wall clock end time in nanoseconds from the epoch
     */
    public long getEndClockEpochNanos() {
        return endClockEpochNanos;
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
     * @return total (allocated high watermark) memory difference between time of completion and creation
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
     * @return true if this nugget was interrupted by an abort() call.
     */
    public boolean wasInterrupted() {
        return state == QueryState.INTERRUPTED;
    }

    /**
     * Ensure this nugget gets logged, alongside its stack of nesting operations.
     */
    void setShouldLogThisAndStackParents() {
        shouldLogThisAndStackParents = true;
    }

    /**
     * @return true if this nugget triggers the logging of itself and every other nugget in its stack of nesting
     *         operations.
     */
    boolean shouldLogThisAndStackParents() {
        return shouldLogThisAndStackParents;
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
        if (shouldLogThisAndStackParents) {
            return true;
        }

        // Nuggets will have a null value for end time if they weren't closed for a RUNNING query; this is an abnormal
        // condition and the nugget should be logged
        if (endClockEpochNanos == NULL_LONG) {
            return true;
        }

        if (isUninstrumented) {
            return UNINSTRUMENTED_LOG_THRESHOLD.shouldLog(getUsageNanos());
        } else {
            return LOG_THRESHOLD.shouldLog(getUsageNanos());
        }
    }
}
