/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.Function;
import io.deephaven.base.Procedure;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.exceptions.QueryCancellationException;
import io.deephaven.db.tables.Table;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.profiling.ThreadProfiler;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.deephaven.db.tables.lang.DBLanguageFunctionUtil.minus;
import static io.deephaven.db.tables.lang.DBLanguageFunctionUtil.plus;

/**
 * Query performance instrumentation tools. Manages a hierarchy of {@link QueryPerformanceNugget}
 * instances.
 * <p>
 * Thread-safety note: This used to be thread-safe only by virtue of using a thread-local instance.
 * Now it's aggressively synchronized so we can abort it from outside the "owner" thread.
 */
public class QueryPerformanceRecorder implements Serializable {

    public static final String UNINSTRUMENTED_CODE_DESCRIPTION = "Uninstrumented code";

    private static final long serialVersionUID = 2L;
    private static final String[] packageFilters;

    private QueryPerformanceNugget queryNugget;
    private final ArrayList<QueryPerformanceNugget> operationNuggets = new ArrayList<>();

    private QueryState state;
    private transient QueryPerformanceNugget catchAllNugget;
    private final transient Deque<QueryPerformanceNugget> userNuggetStack = new ArrayDeque<>();

    private static final AtomicInteger queriesProcessed = new AtomicInteger(0);

    private static final ThreadLocal<QueryPerformanceRecorder> theLocal =
        ThreadLocal.withInitial(QueryPerformanceRecorder::new);
    private static final ThreadLocal<MutableLong> poolAllocatedBytes = ThreadLocal.withInitial(
        () -> new MutableLong(ThreadProfiler.DEFAULT.memoryProfilingAvailable() ? 0L
            : io.deephaven.util.QueryConstants.NULL_LONG));
    private static final ThreadLocal<String> cachedCallsite = new ThreadLocal<>();

    static {
        final Configuration config = Configuration.getInstance();
        final Set<String> filters = new HashSet<>();

        final String propVal =
            config.getProperty("QueryPerformanceRecorder.packageFilter.internal");
        final URL path = QueryPerformanceRecorder.class.getResource("/" + propVal);
        if (path == null) {
            throw new RuntimeException(
                "Can not locate package filter file " + propVal + " in classpath");
        }

        try (final BufferedReader reader =
            new BufferedReader(new InputStreamReader(path.openStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    filters.add(line);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error reading file " + propVal, e);
        }

        packageFilters = filters.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    }

    public static QueryPerformanceRecorder getInstance() {
        return theLocal.get();
    }

    public static void resetInstance() {
        // clear interrupted - because this is a good place to do it - no cancellation exception
        // here though
        // noinspection ResultOfMethodCallIgnored
        Thread.interrupted();
        theLocal.remove();
    }

    /**
     * Start a query.
     * 
     * @param description A description for the query.
     *
     * @return a unique evaluation number to identify this query execution.
     */
    public synchronized int startQuery(final String description) {
        clear();
        final int evaluationNumber = queriesProcessed.getAndIncrement();
        queryNugget = new QueryPerformanceNugget(evaluationNumber, description);
        state = QueryState.RUNNING;
        startCatchAll(evaluationNumber);
        return evaluationNumber;
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
        return queryNugget.done(this);
    }

    private void startCatchAll(final int evaluationNumber) {
        catchAllNugget = new QueryPerformanceNugget(
            evaluationNumber, 0, UNINSTRUMENTED_CODE_DESCRIPTION, false, QueryConstants.NULL_LONG);
    }

    private void stopCatchAll(final boolean abort) {
        final boolean shouldLog;
        if (abort) {
            shouldLog = catchAllNugget.abort(this);
        } else {
            shouldLog = catchAllNugget.done(this);
        }
        if (shouldLog) {
            operationNuggets.add(catchAllNugget);
        }
        catchAllNugget = null;
    }

    /**
     * @param name the nugget name
     * @return A new QueryPerformanceNugget to encapsulate user query operations. done() must be
     *         called on the nugget.
     */
    public QueryPerformanceNugget getNugget(String name) {
        return getNugget(name, QueryConstants.NULL_LONG);
    }

    /**
     * @param name the nugget name
     * @param inputSize the nugget's input size
     * @return A new QueryPerformanceNugget to encapsulate user query operations. done() must be
     *         called on the nugget.
     */
    public synchronized QueryPerformanceNugget getNugget(final String name, final long inputSize) {
        if (state != QueryState.RUNNING) {
            return QueryPerformanceNugget.DUMMY_NUGGET;
        }
        if (Thread.interrupted()) {
            throw new QueryCancellationException("interrupted in QueryPerformanceNugget");
        }
        if (catchAllNugget != null) {
            stopCatchAll(false);
        }
        final QueryPerformanceNugget nugget = new QueryPerformanceNugget(
            queryNugget.getEvaluationNumber(), userNuggetStack.size(),
            name, true, inputSize);
        operationNuggets.add(nugget);
        userNuggetStack.addLast(nugget);
        return nugget;
    }

    /**
     * <b>Note:</b> Do not call this directly - it's for nugget use only. Call nugget.done(),
     * instead. TODO: Reverse the disclaimer above - I think it's much better for the recorder to
     * support done/abort(nugget), rather than continuing to have the nugget support
     * done/abort(recorder).
     * 
     * @param nugget the nugget to be released
     * @return If the nugget passes criteria for logging.
     */
    synchronized boolean releaseNugget(QueryPerformanceNugget nugget) {
        boolean shouldLog = nugget.shouldLogNugget(nugget == catchAllNugget);
        if (!nugget.isUser()) {
            return shouldLog;
        }

        final QueryPerformanceNugget removed = userNuggetStack.removeLast();
        if (nugget != removed) {
            throw new IllegalStateException("Released query performance nugget " + nugget + " ("
                + System.identityHashCode(nugget) +
                ") didn't match the top of the user nugget stack " + removed + " ("
                + System.identityHashCode(removed) +
                ") - did you follow the correct try/finally pattern?");
        }

        if (removed.shouldLogMenAndStackParents()) {
            shouldLog = true;
            if (userNuggetStack.size() > 0) {
                userNuggetStack.getLast().setShouldLogMeAndStackParents();
            }
        }
        if (!shouldLog) {
            // If we have filtered this nugget, by our filter design we will also have filtered any
            // nuggets it encloses.
            // This means it *must* be the last entry in operationNuggets, so we can safely remove
            // it in O(1).
            final QueryPerformanceNugget lastNugget =
                operationNuggets.remove(operationNuggets.size() - 1);
            if (nugget != lastNugget) {
                throw new IllegalStateException("Filtered query performance nugget " + nugget + " ("
                    + System.identityHashCode(nugget) +
                    ") didn't match the last operation nugget " + lastNugget + " ("
                    + System.identityHashCode(lastNugget) +
                    ")");
            }
        }

        if (userNuggetStack.isEmpty() && queryNugget != null && state == QueryState.RUNNING) {
            startCatchAll(queryNugget.getEvaluationNumber());
        }

        return shouldLog;
    }

    public interface EntrySetter {
        void set(int evaluationNumber, int operationNumber, boolean uninstrumented);
    }

    // returns true if uninstrumented code data was captured.
    public void setQueryData(final EntrySetter setter) {
        final int evaluationNumber;
        final int operationNumber;
        boolean uninstrumented = false;
        synchronized (this) {
            if (state != QueryState.RUNNING) {
                setter.set(QueryConstants.NULL_INT, QueryConstants.NULL_INT, false);
                return;
            }
            evaluationNumber = queryNugget.getEvaluationNumber();
            operationNumber = operationNuggets.size();
            if (operationNumber > 0) {
                // ensure UPL and QOPL are consistent/joinable.
                if (userNuggetStack.size() > 0) {
                    userNuggetStack.getLast().setShouldLogMeAndStackParents();
                } else {
                    uninstrumented = true;
                    if (catchAllNugget != null) {
                        catchAllNugget.setShouldLogMeAndStackParents();
                    }
                }
            }
        }
        setter.set(evaluationNumber, operationNumber, uninstrumented);
    }

    private void clear() {
        queryNugget = null;
        catchAllNugget = null;
        operationNuggets.clear();
        userNuggetStack.clear();
    }

    public synchronized QueryPerformanceNugget getQueryLevelPerformanceData() {
        return queryNugget;
    }

    public synchronized List<QueryPerformanceNugget> getOperationLevelPerformanceData() {
        return operationNuggets;
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
            timeNanos[i] = operationNuggets.get(i).getTotalTimeNanos();
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

    /**
     * Record a single-threaded operation's allocations as "pool" allocated memory attributable to
     * the current thread.
     *
     * @param operation The operation to record allocation for
     * @return The result of the operation.
     */
    public static <RESULT_TYPE> RESULT_TYPE recordPoolAllocation(
        @NotNull final Supplier<RESULT_TYPE> operation) {
        final long startThreadAllocatedBytes =
            ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
        try {
            return operation.get();
        } finally {
            final long endThreadAllocatedBytes =
                ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
            final MutableLong poolAllocatedBytesForCurrentThread = poolAllocatedBytes.get();
            poolAllocatedBytesForCurrentThread
                .setValue(plus(poolAllocatedBytesForCurrentThread.longValue(),
                    minus(endThreadAllocatedBytes, startThreadAllocatedBytes)));
        }
    }

    /**
     * Get the total bytes of pool-allocated memory attributed to this thread via
     * {@link #recordPoolAllocation(Supplier)}.
     *
     * @return The total bytes of pool-allocated memory attributed to this thread.
     */
    public static long getPoolAllocatedBytesForCurrentThread() {
        return poolAllocatedBytes.get().longValue();
    }

    public static String getCallerLine() {
        String callerLineCandidate = cachedCallsite.get();

        if (callerLineCandidate == null) {
            final StackTraceElement[] stack = (new Exception()).getStackTrace();
            for (int i = stack.length - 1; i > 0; i--) {
                final String className = stack[i].getClassName();

                if (className.startsWith("io.deephaven.db.util.GroovyDeephavenSession")) {
                    callerLineCandidate = "Groovy Script";
                } else if (Arrays.stream(packageFilters).noneMatch(className::startsWith)) {
                    callerLineCandidate = stack[i].getFileName() + ":" + stack[i].getLineNumber();
                }
            }
        }

        return callerLineCandidate == null ? "Internal" : callerLineCandidate;
    }

    /*------------------------------------------------------------------------------------------------------------------
     * TODO: the following execute-around methods might be better in a separate class or interface
      */

    private static void finishAndClear(QueryPerformanceNugget nugget, boolean needClear) {
        if (nugget != null) {
            nugget.done();
        }

        if (needClear) {
            clearCallsite();
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     * 
     * @param name the nugget name
     * @param r the stuff to run
     */
    public static void withNugget(final String name, final Procedure.Nullary r) {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;

        try {
            nugget = getInstance().getNugget(name);
            r.call();
        } finally {
            finishAndClear(nugget, needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param name the nugget name
     * @param r the stuff to run
     * @return the result of the stuff to run
     */
    public static <T> T withNugget(final String name, final Supplier<T> r) {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;

        try {
            nugget = getInstance().getNugget(name);
            return r.get();
        } finally {
            finishAndClear(nugget, needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param r the stuff to run
     * @throws T exception of type T
     */
    public static <T extends Exception> void withNuggetThrowing(final String name,
        final Procedure.ThrowingNullary<T> r) throws T {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;
        try {
            nugget = getInstance().getNugget(name);
            r.call();
        } finally {
            finishAndClear(nugget, needClear);
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
    public static <R, ExceptionType extends Exception> R withNuggetThrowing(final String name,
        final Function.ThrowingNullary<R, ExceptionType> r) throws ExceptionType {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;
        try {
            nugget = getInstance().getNugget(name);
            return r.call();
        } finally {
            finishAndClear(nugget, needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     * 
     * @param name the nugget name
     * @param r the stuff to run
     */
    public static void withNugget(final String name, final long inputSize,
        final Procedure.Nullary r) {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;
        try {
            nugget = getInstance().getNugget(name, inputSize);
            r.call();
        } finally {
            finishAndClear(nugget, needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param name the nugget name
     * @param r the stuff to run
     * @return the result of the stuff to run
     */
    public static <T> T withNugget(final String name, final long inputSize, final Supplier<T> r) {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;
        try {
            nugget = getInstance().getNugget(name, inputSize);
            return r.get();
        } finally {
            finishAndClear(nugget, needClear);
        }
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param r the stuff to run
     * @throws T exception of type T
     */
    @SuppressWarnings("unused")
    public static <T extends Exception> void withNuggetThrowing(final String name,
        final long inputSize, final Procedure.ThrowingNullary<T> r) throws T {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;
        try {
            nugget = getInstance().getNugget(name, inputSize);
            r.call();
        } finally {
            finishAndClear(nugget, needClear);
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
    public static <R, ExceptionType extends Exception> R withNuggetThrowing(final String name,
        final long inputSize, final Function.ThrowingNullary<R, ExceptionType> r)
        throws ExceptionType {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;
        try {
            nugget = getInstance().getNugget(name, inputSize);
            return r.call();
        } finally {
            finishAndClear(nugget, needClear);
        }
    }

    /**
     * <p>
     * Attempt to set the thread local callsite so that invocations of {@link #getCallerLine()} will
     * not spend time trying to recompute.
     * </p>
     *
     * <p>
     * This method returns a boolean if the value was successfully set. In the event this returns
     * true, it's the responsibility of the caller to invoke {@link #clearCallsite()} when the
     * operation is complete.
     * </p>
     *
     * <p>
     * It is good practice to do this with try{} finally{} block
     * </p>
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
     * @return true if successfully set, false otherwise/
     */
    public static boolean setCallsite(String callsite) {
        if (cachedCallsite.get() == null) {
            cachedCallsite.set(callsite);
            return true;
        }

        return false;
    }

    /**
     * <p>
     * Attempt to compute and set the thread local callsite so that invocations of
     * {@link #getCallerLine()} will not spend time trying to recompute.
     * </p>
     *
     * <p>
     * Users should follow the best practice as described by {@link #setCallsite(String)}
     * </p>
     *
     * @return true if the callsite was computed and set.
     */
    public static boolean setCallsite() {
        // This is very similar to the other getCallsite, but we don't want to invoke
        // getCallerLine() unless we
        // really need to.
        if (cachedCallsite.get() == null) {
            cachedCallsite.set(getCallerLine());
            return true;
        }

        return false;
    }

    /**
     * Clear any previously set callsite. See {@link #setCallsite(String)}
     */
    public static void clearCallsite() {
        cachedCallsite.remove();
    }
}
