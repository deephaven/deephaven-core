/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Query performance instrumentation implementation. Manages a hierarchy of {@link QueryPerformanceNugget} instances.
 * <p>
 * Many methods are synchronized to 1) support external abortion of query and 2) for scenarios where the query is
 * suspended and resumed on another thread.
 */
public class QueryPerformanceRecorderImpl extends QueryPerformanceRecorder {

    private final QueryPerformanceNugget queryNugget;
    private final QueryPerformanceNugget.Factory nuggetFactory;
    private final ArrayList<QueryPerformanceNugget> operationNuggets = new ArrayList<>();
    private final Deque<QueryPerformanceNugget> userNuggetStack = new ArrayDeque<>();

    private QueryState state;
    private QueryPerformanceNugget catchAllNugget;

    /**
     * Creates a new QueryPerformanceRecorderImpl and starts the query.
     *
     * @param description a description for the query
     * @param nuggetFactory the factory to use for creating new nuggets
     */
    public QueryPerformanceRecorderImpl(
            @NotNull final String description,
            @NotNull final QueryPerformanceNugget.Factory nuggetFactory) {
        this(nuggetFactory.createForQuery(queriesProcessed.getAndIncrement(), description), nuggetFactory);
    }

    /**
     * Constructor for a sub-query.
     *
     * @param description a description for the query
     * @param parent the parent query
     * @param nuggetFactory the factory to use for creating new nuggets
     */
    public QueryPerformanceRecorderImpl(
            @NotNull final String description,
            @NotNull final QueryPerformanceRecorderImpl parent,
            @NotNull final QueryPerformanceNugget.Factory nuggetFactory) {
        this(nuggetFactory.createForSubQuery(
                parent.queryNugget, queriesProcessed.getAndIncrement(), description), nuggetFactory);
    }

    /**
     * @param queryNugget The newly constructed query level queryNugget.
     * @param nuggetFactory The factory to use for creating new nuggets.
     */
    private QueryPerformanceRecorderImpl(
            @NotNull final QueryPerformanceNugget queryNugget,
            @NotNull final QueryPerformanceNugget.Factory nuggetFactory) {
        this.queryNugget = queryNugget;
        this.nuggetFactory = nuggetFactory;
        state = QueryState.RUNNING;
        startCatchAll();
        Assert.eqTrue(QueryPerformanceRecorder.getInstance() == DUMMY_RECORDER,
                "QueryPerformanceRecorder.getInstance() == DUMMY_RECORDER");
        QueryPerformanceRecorder.theLocal.set(this);
    }

    /**
     * Abort a query.
     */
    public synchronized void abortQuery() {
        if (state != QueryState.RUNNING) {
            return;
        }
        state = QueryState.INTERRUPTED;
        if (catchAllNugget != null) {
            stopCatchAll(true);
        } else {
            while (!userNuggetStack.isEmpty()) {
                userNuggetStack.peekLast().abort(this);
            }
        }
        queryNugget.abort(this);
    }

    /**
     * Return the query's current state
     *
     * @return the query's state or null if it isn't initialized yet
     */
    public synchronized QueryState getState() {
        return state;
    }

    /**
     * End a query.
     */
    public synchronized boolean endQuery() {
        if (state != QueryState.RUNNING) {
            return false;
        }

        state = QueryState.FINISHED;
        Assert.neqNull(catchAllNugget, "catchAllNugget");
        Assert.neqNull(queryNugget, "queryNugget");
        stopCatchAll(false);

        // note that we do not resetInstance in here as that should be done from a finally-block
        return queryNugget.done(this);
    }

    /**
     * Suspends a query.
     * <p>
     * This resets the thread local and assumes that this performance nugget may be resumed on another thread.
     */
    public synchronized void suspendQuery() {
        if (state != QueryState.RUNNING) {
            throw new IllegalStateException("Can't suspend a query that isn't running");
        }

        final QueryPerformanceRecorder threadLocalInstance = getInstance();
        if (threadLocalInstance != this) {
            throw new IllegalStateException("Can't suspend a query that doesn't belong to this thread");
        }

        state = QueryState.SUSPENDED;
        Assert.neqNull(catchAllNugget, "catchAllNugget");
        stopCatchAll(false);
        queryNugget.onBaseEntryEnd();

        // uninstall this instance from the thread local
        resetInstance();
    }

    /**
     * Resumes a suspend query.
     * <p>
     * It is an error to resume a query while another query is running on this thread.
     *
     * @return this
     */
    public synchronized QueryPerformanceRecorderImpl resumeQuery() {
        if (state != QueryState.SUSPENDED) {
            throw new IllegalStateException("Can't resume a query that isn't suspended");
        }

        final QueryPerformanceRecorder threadLocalInstance = getInstance();
        if (threadLocalInstance != DUMMY_RECORDER) {
            throw new IllegalStateException("Can't resume a query while another query is in operation");
        }
        QueryPerformanceRecorder.theLocal.set(this);

        queryNugget.onBaseEntryStart();
        state = QueryState.RUNNING;
        Assert.eqNull(catchAllNugget, "catchAllNugget");
        startCatchAll();
        return this;
    }

    private void startCatchAll() {
        catchAllNugget = nuggetFactory.createForCatchAll(queryNugget, operationNuggets.size());
    }

    private void stopCatchAll(final boolean abort) {
        final boolean shouldLog;
        if (abort) {
            shouldLog = catchAllNugget.abort(this);
        } else {
            shouldLog = catchAllNugget.done(this);
        }
        if (shouldLog) {
            Assert.eq(operationNuggets.size(), "operationsNuggets.size()",
                    catchAllNugget.getOperationNumber(), "catchAllNugget.getOperationNumber()");
            operationNuggets.add(catchAllNugget);
        }
        catchAllNugget = null;
    }

    /**
     * @param name the nugget name
     * @return A new QueryPerformanceNugget to encapsulate user query operations. done() must be called on the nugget.
     */
    public QueryPerformanceNugget getNugget(@NotNull final String name) {
        return getNugget(name, QueryConstants.NULL_LONG);
    }

    /**
     * @param name the nugget name
     * @param inputSize the nugget's input size
     * @return A new QueryPerformanceNugget to encapsulate user query operations. done() must be called on the nugget.
     */
    public synchronized QueryPerformanceNugget getNugget(@NotNull final String name, final long inputSize) {
        Assert.eq(state, "state", QueryState.RUNNING, "QueryState.RUNNING");
        if (Thread.interrupted()) {
            throw new CancellationException("interrupted in QueryPerformanceNugget");
        }
        if (catchAllNugget != null) {
            stopCatchAll(false);
        }
        final QueryPerformanceNugget parent = userNuggetStack.isEmpty() ? queryNugget : userNuggetStack.getLast();
        final QueryPerformanceNugget nugget = nuggetFactory.createForOperation(
                parent, operationNuggets.size(), name, inputSize);
        operationNuggets.add(nugget);
        userNuggetStack.addLast(nugget);
        return nugget;
    }

    /**
     * <b>Note:</b> Do not call this directly - it's for nugget use only. Call {@link QueryPerformanceNugget#done()} or
     * {@link QueryPerformanceNugget#close()} instead.
     *
     * @param nugget the nugget to be released
     * @return If the nugget passes criteria for logging.
     */
    synchronized boolean releaseNugget(@NotNull final QueryPerformanceNugget nugget) {
        boolean shouldLog = nugget.shouldLogNugget(nugget == catchAllNugget);
        if (!nugget.isUser()) {
            return shouldLog;
        }

        final QueryPerformanceNugget removed = userNuggetStack.removeLast();
        if (nugget != removed) {
            throw new IllegalStateException(
                    "Released query performance nugget " + nugget + " (" + System.identityHashCode(nugget) +
                            ") didn't match the top of the user nugget stack " + removed + " ("
                            + System.identityHashCode(removed) +
                            ") - did you follow the correct try/finally pattern?");
        }

        shouldLog |= removed.shouldLogThisAndStackParents();

        if (shouldLog) {
            // It is entirely possible, with parallelization, that this nugget should be logged while the outer nugget
            // has a wall clock time less than the threshold for logging. If we ever want to log this nugget, we must
            // log
            // all of its parents as well regardless of the shouldLogNugget call result.
            if (!userNuggetStack.isEmpty()) {
                userNuggetStack.getLast().setShouldLogThisAndStackParents();
            }
        } else {
            // If we have filtered this nugget, by our filter design we will also have filtered any nuggets it encloses.
            // This means it *must* be the last entry in operationNuggets, so we can safely remove it in O(1).
            final QueryPerformanceNugget lastNugget = operationNuggets.remove(operationNuggets.size() - 1);
            if (nugget != lastNugget) {
                throw new IllegalStateException(
                        "Filtered query performance nugget " + nugget + " (" + System.identityHashCode(nugget) +
                                ") didn't match the last operation nugget " + lastNugget + " ("
                                + System.identityHashCode(lastNugget) +
                                ")");
            }
        }

        if (userNuggetStack.isEmpty() && queryNugget != null && state == QueryState.RUNNING) {
            startCatchAll();
        }

        return shouldLog;
    }

    @Override
    public synchronized QueryPerformanceNugget getOuterNugget() {
        return userNuggetStack.peekLast();
    }

    @Override
    public void setQueryData(final EntrySetter setter) {
        final long evaluationNumber;
        final int operationNumber;
        boolean uninstrumented = false;
        synchronized (this) {
            // we should never be called if we're not running
            Assert.eq(state, "state", QueryState.RUNNING, "QueryState.RUNNING");
            evaluationNumber = queryNugget.getEvaluationNumber();
            operationNumber = operationNuggets.size();
            if (operationNumber > 0) {
                // ensure UPL and QOPL are consistent/joinable.
                if (!userNuggetStack.isEmpty()) {
                    userNuggetStack.getLast().setShouldLogThisAndStackParents();
                } else {
                    uninstrumented = true;
                    if (catchAllNugget != null) {
                        catchAllNugget.setShouldLogThisAndStackParents();
                    }
                }
            }
        }
        setter.set(evaluationNumber, operationNumber, uninstrumented);
    }

    @Override
    public synchronized QueryPerformanceNugget getQueryLevelPerformanceData() {
        return queryNugget;
    }

    @Override
    public synchronized List<QueryPerformanceNugget> getOperationLevelPerformanceData() {
        return operationNuggets;
    }

    public void accumulate(@NotNull final QueryPerformanceRecorderImpl subQuery) {
        queryNugget.accumulate(subQuery.queryNugget);
    }

    @SuppressWarnings("unused")
    public synchronized Table getTimingResultsAsTable() {
        final int count = operationNuggets.size();
        final String[] names = new String[count];
        final Long[] timeNanos = new Long[count];
        final String[] callerLine = new String[count];
        final Boolean[] isTopLevel = new Boolean[count];
        final Boolean[] isCompileTime = new Boolean[count];

        for (int i = 0; i < operationNuggets.size(); i++) {
            timeNanos[i] = operationNuggets.get(i).getUsageNanos();
            names[i] = operationNuggets.get(i).getName();
            callerLine[i] = operationNuggets.get(i).getCallerLine();
            isTopLevel[i] = operationNuggets.get(i).isTopLevel();
            isCompileTime[i] = operationNuggets.get(i).getName().startsWith("Compile:");
        }
        return TableTools.newTable(
                TableTools.col("names", names),
                TableTools.col("line", callerLine),
                TableTools.col("timeNanos", timeNanos),
                TableTools.col("isTopLevel", isTopLevel),
                TableTools.col("isCompileTime", isCompileTime));
    }
}
