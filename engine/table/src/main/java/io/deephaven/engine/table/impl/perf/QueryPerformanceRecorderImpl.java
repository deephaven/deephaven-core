//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Query performance instrumentation implementation. Manages a hierarchy of {@link QueryPerformanceNugget} instances.
 * <p>
 * Many methods are synchronized to 1) support external abort of query and 2) for scenarios where the query is suspended
 * and resumed on another thread.
 */
public class QueryPerformanceRecorderImpl implements QueryPerformanceRecorder {
    private static final QueryPerformanceLogThreshold LOG_THRESHOLD = new QueryPerformanceLogThreshold("", 1_000_000);
    private static final QueryPerformanceLogThreshold UNINSTRUMENTED_LOG_THRESHOLD =
            new QueryPerformanceLogThreshold("Uninstrumented", 1_000_000_000);

    @Nullable
    private final QueryPerformanceRecorder parent;
    private final QueryPerformanceNugget queryNugget;
    private final QueryPerformanceNugget.Factory nuggetFactory;
    private final ArrayList<QueryPerformanceNugget> operationNuggets = new ArrayList<>();
    private final Deque<QueryPerformanceNugget> userNuggetStack = new ArrayDeque<>();

    private QueryState state = QueryState.NOT_STARTED;
    private volatile boolean hasSubQueries;
    private QueryPerformanceNugget catchAllNugget;
    /**
     * The query that owned the thread when this one was resumed on top of it, to hand the thread back to; null when
     * this query is not installed on a thread, or took an idle thread. Guarded by this, like the rest of the state.
     */
    private QueryPerformanceRecorder outerInstance;
    /** Counts installations, so that a closeable only ever uninstalls the one it was returned for; guarded by this. */
    private int installation;
    /**
     * Whether another query is resumed on top of this one, with this query's accruing entry (the catch-all, or the top
     * user nugget) paused for the duration so that the inner query's time is not charged here too. Guarded by this.
     */
    private boolean pausedForNestedQuery;

    /**
     * Constructs a QueryPerformanceRecorderImpl.
     *
     * @param description a description for the query
     * @param nuggetFactory the factory to use for creating new nuggets
     * @param parent the parent query if it exists
     */
    QueryPerformanceRecorderImpl(
            @NotNull final String description,
            @Nullable final String sessionId,
            @Nullable final QueryPerformanceRecorder parent,
            @NotNull final QueryPerformanceNugget.Factory nuggetFactory) {
        if (parent == null) {
            queryNugget = nuggetFactory.createForQuery(
                    QueryPerformanceRecorderState.QUERIES_PROCESSED.getAndIncrement(), description, sessionId,
                    this::releaseNugget);
        } else {
            queryNugget = nuggetFactory.createForSubQuery(
                    parent.getQueryLevelPerformanceData(),
                    QueryPerformanceRecorderState.QUERIES_PROCESSED.getAndIncrement(), description,
                    this::releaseNugget);
        }
        this.parent = parent;
        this.nuggetFactory = nuggetFactory;
    }

    @Override
    public synchronized void abortQuery() {
        // TODO (https://github.com/deephaven/deephaven-core/issues/53): support out-of-order abort
        if (state != QueryState.RUNNING) {
            return;
        }
        state = QueryState.INTERRUPTED;
        if (pausedForNestedQuery) {
            // a suspended nugget ignores abort(); resume the paused entry so that it closes below
            resumeAccruingEntry();
        }
        if (catchAllNugget != null) {
            stopCatchAll(true);
        } else {
            while (!userNuggetStack.isEmpty()) {
                userNuggetStack.peekLast().abort();
            }
        }
        queryNugget.abort();
    }

    /**
     * Return the query's current state
     *
     * @return the query's state or null if it isn't initialized yet
     */
    @Override
    public synchronized QueryState getState() {
        return state;
    }

    @Override
    public synchronized SafeCloseable startQuery() {
        if (state != QueryState.NOT_STARTED) {
            throw new IllegalStateException("Can't resume a query that has already started");
        }
        return resumeInternal(false);
    }

    @Override
    public synchronized boolean endQuery() {
        if (state != QueryState.RUNNING) {
            if (state != QueryState.INTERRUPTED) {
                // We only allow the query to be RUNNING or INTERRUPTED when we end it; else we are in an illegal state.
                throw new IllegalStateException("Can't end a query that isn't running or interrupted");
            }
            return false;
        }
        checkOwnedByThisThread();
        state = QueryState.FINISHED;
        suspendInternal();

        queryNugget.close();
        if (parent != null) {
            parent.accumulate(this);
        }
        return shouldLogNugget(queryNugget) || !operationNuggets.isEmpty() || hasSubQueries;
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
        checkOwnedByThisThread();
        state = QueryState.SUSPENDED;
        suspendInternal();
        queryNugget.onBaseEntryEnd();
    }

    /**
     * A running query may only be suspended or ended by the thread it is installed on. Checked before any state
     * changes, so that a rejected call leaves the query as it was for its owner.
     */
    private void checkOwnedByThisThread() {
        if (QueryPerformanceRecorderState.getInstance() != this) {
            throw new IllegalStateException("Query doesn't belong to this thread");
        }
    }

    private void suspendInternal() {
        Assert.neqNull(catchAllNugget, "catchAllNugget");
        stopCatchAll(false);

        uninstall(installation);
    }

    /**
     * Uninstalls this recorder from the current thread and hands the thread back to the query that was running when
     * this one was resumed on top of it, if any. A no-op unless {@code forInstallation} is the current installation and
     * it is still on this thread, so that the closeable returned by {@link #resumeInternal} is safe to close after
     * {@link #endQuery} or {@link #suspendQuery}, and cannot disturb a later installation.
     */
    private synchronized void uninstall(final int forInstallation) {
        if (forInstallation != installation || QueryPerformanceRecorderState.getInstance() != this) {
            return;
        }
        final QueryPerformanceRecorder outer = outerInstance;
        outerInstance = null;
        QueryPerformanceRecorderState.resetInstance();
        if (outer != null) {
            QueryPerformanceRecorderState.setInstance(outer);
            if (outer instanceof QueryPerformanceRecorderImpl) {
                ((QueryPerformanceRecorderImpl) outer).onNestedQueryLeft();
            }
        }
    }

    /**
     * Pauses this query's accruing entry while another query runs on top of it on this thread. An aborted query stays
     * installed until its scope closes, with its entries already closed, so there is nothing to pause for it.
     */
    private synchronized void onNestedQueryResumed() {
        if (state == QueryState.INTERRUPTED) {
            return;
        }
        Assert.eq(state, "state", QueryState.RUNNING, "QueryState.RUNNING");
        Assert.eqFalse(pausedForNestedQuery, "pausedForNestedQuery");
        pausedForNestedQuery = true;
        accruingEntry().onBaseEntryEnd();
    }

    /**
     * Resumes this query's accruing entry once the query that was running on top of it has handed the thread back,
     * unless this query was aborted meanwhile, which already closed the entry.
     */
    private synchronized void onNestedQueryLeft() {
        if (!pausedForNestedQuery) {
            return;
        }
        resumeAccruingEntry();
    }

    private void resumeAccruingEntry() {
        pausedForNestedQuery = false;
        accruingEntry().onBaseEntryStart();
    }

    /** The entry currently charged for this query's time: the catch-all, or else the innermost open user nugget. */
    private QueryPerformanceNugget accruingEntry() {
        return catchAllNugget != null ? catchAllNugget : userNuggetStack.peekLast();
    }

    /**
     * Resumes a suspend query.
     * <p>
     * The query may be resumed on a thread that is already running another query; that outer query gets the thread back
     * as soon as this one ends or suspends.
     *
     * @return a closeable that restores the query that was running on this thread before, if any
     */
    public synchronized SafeCloseable resumeQuery() {
        if (state != QueryState.SUSPENDED) {
            throw new IllegalStateException("Can't resume a query that isn't suspended");
        }

        return resumeInternal(true);
    }

    /**
     * Installs this recorder on the current thread and marks the query running.
     *
     * @param allowNesting whether this query may take over a thread that is already running another query: a resumed
     *        query may, a newly started one may not
     * @return a closeable that hands the thread back to the query that was running before, if any
     */
    private SafeCloseable resumeInternal(final boolean allowNesting) {
        final QueryPerformanceRecorder current = QueryPerformanceRecorderState.getInstance();
        // an installed query is RUNNING, and only a NOT_STARTED or SUSPENDED one gets here; handing the thread back
        // to ourselves would otherwise leave it owned forever
        Assert.neq(current, "current", this, "this");
        if (!allowNesting && current != QueryPerformanceRecorderState.DUMMY_RECORDER) {
            throw new IllegalStateException("Can't start a query while another query is in operation");
        }
        outerInstance = current == QueryPerformanceRecorderState.DUMMY_RECORDER ? null : current;
        if (outerInstance instanceof QueryPerformanceRecorderImpl) {
            ((QueryPerformanceRecorderImpl) outerInstance).onNestedQueryResumed();
        }
        final int thisInstallation = ++installation;
        QueryPerformanceRecorderState.setInstance(this);

        queryNugget.onBaseEntryStart();
        state = QueryState.RUNNING;
        Assert.eqNull(catchAllNugget, "catchAllNugget");
        startCatchAll();

        // ending or suspending the query hands the thread back itself; this covers an exit without either
        return () -> uninstall(thisInstallation);
    }

    private void startCatchAll() {
        catchAllNugget = nuggetFactory.createForCatchAll(queryNugget, operationNuggets.size(), this::releaseNugget);
        catchAllNugget.onBaseEntryStart();
    }

    private void stopCatchAll(final boolean abort) {
        if (abort) {
            catchAllNugget.abort();
        } else {
            catchAllNugget.close();
        }
        if (catchAllNugget.shouldLog()) {
            Assert.eq(operationNuggets.size(), "operationsNuggets.size()",
                    catchAllNugget.getOperationNumber(), "catchAllNugget.getOperationNumber()");
            operationNuggets.add(catchAllNugget);
        }
        catchAllNugget = null;
    }

    @Override
    public synchronized QueryPerformanceNugget getNugget(@NotNull final String name, final long inputSize) {
        return getNuggetInternal(parent -> nuggetFactory.createForOperation(
                parent, operationNuggets.size(), name, inputSize, this::releaseNugget));
    }

    @Override
    public QueryPerformanceNugget getCompilationNugget(@NotNull final String name) {
        return getNuggetInternal(parent -> nuggetFactory.createForCompilation(
                parent, operationNuggets.size(), name, this::releaseNugget));
    }

    private QueryPerformanceNugget getNuggetInternal(
            @NotNull final Function<QueryPerformanceNugget, QueryPerformanceNugget> nuggetSupplier) {
        Assert.eq(state, "state", QueryState.RUNNING, "QueryState.RUNNING");
        if (Thread.interrupted()) {
            throw new CancellationException("interrupted in QueryPerformanceNugget");
        }
        if (catchAllNugget != null) {
            stopCatchAll(false);
        }

        final QueryPerformanceNugget parent;
        if (userNuggetStack.isEmpty()) {
            parent = queryNugget;
        } else {
            parent = userNuggetStack.peekLast();
            parent.onBaseEntryEnd();
        }

        final QueryPerformanceNugget nugget = nuggetSupplier.apply(parent);
        nugget.onBaseEntryStart();
        operationNuggets.add(nugget);
        userNuggetStack.addLast(nugget);
        return nugget;
    }

    /**
     * This is our onCloseCallback from the nugget.
     *
     * @param nugget the nugget to be released
     */
    private synchronized void releaseNugget(@NotNull final QueryPerformanceNugget nugget) {
        final boolean shouldLog = shouldLogNugget(nugget);
        if (!nugget.isUser()) {
            return;
        }

        final QueryPerformanceNugget removed = userNuggetStack.removeLast();
        if (nugget != removed) {
            throw new IllegalStateException(
                    "Released query performance nugget " + nugget + " (" + System.identityHashCode(nugget) +
                            ") didn't match the top of the user nugget stack " + removed + " ("
                            + System.identityHashCode(removed) +
                            ") - did you follow the correct try/finally pattern?");
        }

        // accumulate into the parent and resume it
        if (!userNuggetStack.isEmpty()) {
            final QueryPerformanceNugget parent = userNuggetStack.getLast();
            parent.accumulate(nugget);

            if (shouldLog) {
                parent.setShouldLog();
            }

            // resume the parent
            parent.onBaseEntryStart();
        } else {
            queryNugget.accumulate(nugget);
        }

        if (!shouldLog) {
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
    }

    private boolean shouldLogNugget(@NotNull QueryPerformanceNugget nugget) {
        if (nugget.shouldLog()) {
            return true;
        } else if (nugget.getEndClockEpochNanos() == NULL_LONG) {
            // Nuggets will have a null value for end time if they weren't closed for a RUNNING query; this is an
            // abnormal condition and the nugget should be logged
            return true;
        } else if (nugget == catchAllNugget) {
            return UNINSTRUMENTED_LOG_THRESHOLD.shouldLog(nugget.getUsageNanos(), nugget.getDataReadCount(),
                    nugget.getMetadataOperationCount());
        } else {
            return LOG_THRESHOLD.shouldLog(nugget.getUsageNanos(), nugget.getDataReadCount(),
                    nugget.getMetadataOperationCount());
        }
    }

    @Override
    public synchronized QueryPerformanceNugget getEnclosingNugget() {
        if (userNuggetStack.isEmpty()) {
            Assert.neqNull(catchAllNugget, "catchAllNugget");
            return catchAllNugget;
        }
        return userNuggetStack.peekLast();
    }

    @Override
    public void supplyQueryData(final @NotNull QueryDataConsumer consumer) {
        final long evaluationNumber;
        final int operationNumber;
        final boolean uninstrumented;
        synchronized (this) {
            // we should never be called if we're not running
            Assert.eq(state, "state", QueryState.RUNNING, "QueryState.RUNNING");

            final QueryPerformanceNugget nugget = getEnclosingNugget();
            evaluationNumber = nugget.getEvaluationNumber();
            operationNumber = nugget.getOperationNumber();
            uninstrumented = nugget == catchAllNugget;

            // ensure UPL and QOPL are consistent/joinable.
            nugget.setShouldLog();
        }
        consumer.accept(evaluationNumber, operationNumber, uninstrumented);
    }

    @Override
    public QueryPerformanceNugget getQueryLevelPerformanceData() {
        return queryNugget;
    }

    @Override
    public List<QueryPerformanceNugget> getOperationLevelPerformanceData() {
        return operationNuggets;
    }

    @Override
    public void accumulate(@NotNull final QueryPerformanceRecorder subQuery) {
        hasSubQueries = true;
        queryNugget.accumulate(subQuery.getQueryLevelPerformanceData());
    }

    @Override
    public boolean hasSubQueries() {
        return hasSubQueries;
    }
}
