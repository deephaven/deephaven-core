/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.function.ThrowingRunnable;
import io.deephaven.util.function.ThrowingSupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Supplier;

/**
 * Query performance instrumentation tools. Manages a hierarchy of {@link QueryPerformanceNugget} instances.
 */
public interface QueryPerformanceRecorder {

    String UNINSTRUMENTED_CODE_DESCRIPTION = "Uninstrumented code";

    /////////////////////////////////////
    // Core Engine Instrumentation API //
    /////////////////////////////////////

    static QueryPerformanceRecorder getInstance() {
        return QueryPerformanceRecorderState.getInstance();
    }

    /**
     * Create a nugget at the top of the user stack. May return a {@link QueryPerformanceNugget#DUMMY_NUGGET} if no
     * recorder is installed.
     *
     * @param name the nugget name
     * @return A new QueryPerformanceNugget to encapsulate user query operations. {@link QueryPerformanceNugget#close()}
     *         must be called on the nugget.
     */
    @FinalDefault
    default QueryPerformanceNugget getNugget(@NotNull String name) {
        return getNugget(name, QueryConstants.NULL_LONG);
    }

    /**
     * Create a nugget at the top of the user stack. May return a {@link QueryPerformanceNugget#DUMMY_NUGGET} if no
     * recorder is installed.
     *
     * @param name the nugget name
     * @param inputSize the nugget's input size
     * @return A new QueryPerformanceNugget to encapsulate user query operations. {@link QueryPerformanceNugget#close()}
     *         must be called on the nugget.
     */
    QueryPerformanceNugget getNugget(@NotNull String name, long inputSize);

    /**
     * This is the nugget enclosing the current operation. It may belong to the dummy recorder, or a real one.
     *
     * @return Either a "catch-all" nugget, or the top of the user nugget stack.
     */
    QueryPerformanceNugget getEnclosingNugget();


    interface QueryDataConsumer {
        void accept(long evaluationNumber, int operationNumber, boolean uninstrumented);
    }

    /**
     * Provide current query data via the consumer.
     *
     * @param consumer a callback to receive query data
     */
    void supplyQueryData(@NotNull QueryDataConsumer consumer);

    /**
     * @return The current callsite. This is the last set callsite or the line number of the user's detected callsite.
     */
    static String getCallerLine() {
        return QueryPerformanceRecorderState.getCallerLine();
    }

    /**
     * Attempt to set the thread local callsite so that invocations of {@link #getCallerLine()} will not spend time
     * trying to recompute.
     * <p>
     * This method returns a boolean if the value was successfully set. In the event this returns true, it's the
     * responsibility of the caller to invoke {@link #clearCallsite()} when the operation is complete.
     * <p>
     * It is good practice to do this with try{} finally{} block
     *
     * <pre>
     * final boolean shouldClear = QueryPerformanceRecorder.setCallsite("CALLSITE");
     * try {
     *     // Do work
     * } finally {
     *     if (shouldClear) {
     *         QueryPerformanceRecorder.clearCallsite();
     *     }
     * }
     * </pre>
     *
     * @param callsite The call site to use.
     *
     * @return true if successfully set, false otherwise
     */
    static boolean setCallsite(@NotNull final String callsite) {
        return QueryPerformanceRecorderState.setCallsite(callsite);
    }

    /**
     * Attempt to compute and set the thread local callsite so that invocations of {@link #getCallerLine()} will not
     * spend time trying to recompute.
     * <p>
     * Users should follow the best practice as described by {@link #setCallsite(String)}
     *
     * @return true if the callsite was computed and set.
     */
    static boolean setCallsite() {
        return QueryPerformanceRecorderState.setCallsite();
    }

    /**
     * Clear any previously set callsite. See {@link #setCallsite(String)}
     */
    static void clearCallsite() {
        QueryPerformanceRecorderState.clearCallsite();
    }

    ////////////////////////////////////////////
    // Server-Level Performance Recording API //
    ////////////////////////////////////////////

    /**
     * Construct a QueryPerformanceRecorder for a top-level query.
     *
     * @param description the query description
     * @param nuggetFactory the nugget factory
     * @return a new QueryPerformanceRecorder
     */
    static QueryPerformanceRecorder newQuery(
            @NotNull final String description,
            @Nullable final String sessionId,
            @NotNull final QueryPerformanceNugget.Factory nuggetFactory) {
        return new QueryPerformanceRecorderImpl(description, sessionId, null, nuggetFactory);
    }

    /**
     * Construct a QueryPerformanceRecorder for a sub-level query.
     *
     * @param description the query description
     * @param nuggetFactory the nugget factory
     * @return a new QueryPerformanceRecorder
     */
    static QueryPerformanceRecorder newSubQuery(
            @NotNull final String description,
            @Nullable final QueryPerformanceRecorder parent,
            @NotNull final QueryPerformanceNugget.Factory nuggetFactory) {
        return new QueryPerformanceRecorderImpl(description, null, parent, nuggetFactory);
    }

    /**
     * Return the query's current state
     *
     * @return the query's state
     */
    QueryState getState();

    /**
     * Starts a query.
     * <p>
     * A query is {@link QueryState#RUNNING RUNNING} if it has been started or {@link #resumeQuery() resumed} without a
     * subsequent {@link #endQuery() end}, {@link #suspendQuery() suspend}, or {@link #abortQuery() abort}.
     *
     * @throws IllegalStateException if the query state isn't {@link QueryState#NOT_STARTED NOT_STARTED} or another
     *         query is running on this thread
     */
    SafeCloseable startQuery();

    /**
     * End a query.
     * <p>
     * A query is {@link QueryState#RUNNING RUNNING} if it has been {@link #startQuery() started} or
     * {@link #resumeQuery() resumed} without a subsequent end, {@link #suspendQuery() suspend}, or {@link #abortQuery()
     * abort}.
     *
     * @return whether the query should be logged
     * @throws IllegalStateException if the query state isn't {@link QueryState#RUNNING RUNNING},
     *         {@link QueryState#INTERRUPTED INTERRUPTED}, or was not running on this thread
     */
    boolean endQuery();

    /**
     * Suspends a query.
     * <p>
     * A query is {@link QueryState#RUNNING RUNNING} if it has been {@link #startQuery() started} or
     * {@link #resumeQuery() resumed} without a subsequent {@link #endQuery() end}, suspend, or {@link #abortQuery()
     * abort}.
     *
     * @throws IllegalStateException if the query state isn't {@link QueryState#RUNNING RUNNING} or was not running on
     *         this thread
     */
    void suspendQuery();

    /**
     * Resumes a suspend query.
     * <p>
     * A query is {@link QueryState#RUNNING RUNNING} if it has been {@link #startQuery() started} or resumed without a
     * subsequent {@link #endQuery() end}, {@link #suspendQuery() suspend}, or {@link #abortQuery() abort}.
     *
     * @throws IllegalStateException if the query state isn't {@link QueryState#SUSPENDED SUSPENDED} or another query is
     *         running on this thread
     */
    SafeCloseable resumeQuery();

    /**
     * Abort a query.
     * <p>
     * A query is {@link QueryState#RUNNING RUNNING} if it has been {@link #startQuery() started} or
     * {@link #resumeQuery() resumed} without a subsequent {@link #endQuery() end}, {@link #suspendQuery() suspend}, or
     * abort.
     * <p>
     * Note that this method is invoked out-of-band and does not throw if the query has been completed.
     */
    @SuppressWarnings("unused")
    void abortQuery();

    /**
     * @return the query level performance data
     */
    QueryPerformanceNugget getQueryLevelPerformanceData();

    /**
     * This getter should be called by exclusive owners of the recorder, and never concurrently with mutators.
     *
     * @return A list of loggable operation performance data.
     */
    List<QueryPerformanceNugget> getOperationLevelPerformanceData();

    /**
     * Accumulate performance information from another recorder into this one. The provided recorder will not be
     * mutated.
     *
     * @param subQuery the recorder to accumulate into this
     */
    void accumulate(@NotNull QueryPerformanceRecorder subQuery);

    /**
     * @return whether a sub-query was ever accumulated into this recorder
     */
    @SuppressWarnings("unused")
    boolean hasSubQueries();

    ///////////////////////////////////////////////////
    // Convenience Methods for Recording Performance //
    ///////////////////////////////////////////////////

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param name the nugget name
     * @param r the stuff to run
     */
    static void withNugget(final String name, final Runnable r) {
        final boolean needClear = setCallsite();
        try (final QueryPerformanceNugget ignored = getInstance().getNugget(name)) {
            r.run();
        } finally {
            maybeClearCallsite(needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param name the nugget name
     * @param r the stuff to run
     * @return the result of the stuff to run
     */
    static <T> T withNugget(final String name, final Supplier<T> r) {
        final boolean needClear = setCallsite();
        try (final QueryPerformanceNugget ignored = getInstance().getNugget(name)) {
            return r.get();
        } finally {
            maybeClearCallsite(needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param r the stuff to run
     * @throws T exception of type T
     */
    static <T extends Exception> void withNuggetThrowing(
            final String name,
            final ThrowingRunnable<T> r) throws T {
        final boolean needClear = setCallsite();
        try (final QueryPerformanceNugget ignored = getInstance().getNugget(name)) {
            r.run();
        } finally {
            maybeClearCallsite(needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param name the nugget name
     * @param r the stuff to run
     * @return the result of the stuff to run
     * @throws ExceptionType exception of type ExceptionType
     */
    static <R, ExceptionType extends Exception> R withNuggetThrowing(
            final String name,
            final ThrowingSupplier<R, ExceptionType> r) throws ExceptionType {
        final boolean needClear = setCallsite();
        try (final QueryPerformanceNugget ignored = getInstance().getNugget(name)) {
            return r.get();
        } finally {
            maybeClearCallsite(needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param name the nugget name
     * @param r the stuff to run
     */
    static void withNugget(final String name, final long inputSize, final Runnable r) {
        final boolean needClear = setCallsite();
        try (final QueryPerformanceNugget ignored = getInstance().getNugget(name, inputSize)) {
            r.run();
        } finally {
            maybeClearCallsite(needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param name the nugget name
     * @param r the stuff to run
     * @return the result of the stuff to run
     */
    static <T> T withNugget(final String name, final long inputSize, final Supplier<T> r) {
        final boolean needClear = setCallsite();
        try (final QueryPerformanceNugget ignored = getInstance().getNugget(name, inputSize)) {
            return r.get();
        } finally {
            maybeClearCallsite(needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param r the stuff to run
     * @throws T exception of type T
     */
    @SuppressWarnings("unused")
    static <T extends Exception> void withNuggetThrowing(
            final String name,
            final long inputSize,
            final ThrowingRunnable<T> r) throws T {
        final boolean needClear = setCallsite();
        try (final QueryPerformanceNugget ignored = getInstance().getNugget(name, inputSize)) {
            r.run();
        } finally {
            maybeClearCallsite(needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param name the nugget name
     * @param r the stuff to run
     * @return the result of the stuff to run
     * @throws ExceptionType exception of type ExceptionType
     */
    @SuppressWarnings("unused")
    static <R, ExceptionType extends Exception> R withNuggetThrowing(
            final String name,
            final long inputSize,
            final ThrowingSupplier<R, ExceptionType> r) throws ExceptionType {
        final boolean needClear = setCallsite();
        try (final QueryPerformanceNugget ignored = getInstance().getNugget(name, inputSize)) {
            return r.get();
        } finally {
            maybeClearCallsite(needClear);
        }
    }

    /**
     * Clear the callsite if needed.
     *
     * @param needClear true if the callsite needs to be cleared
     */
    private static void maybeClearCallsite(final boolean needClear) {
        if (needClear) {
            clearCallsite();
        }
    }
}
