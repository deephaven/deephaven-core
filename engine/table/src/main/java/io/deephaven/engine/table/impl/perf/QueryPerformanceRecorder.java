/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.chunk.util.pools.ChunkPoolInstrumentation;
import io.deephaven.engine.updategraph.UpdateGraphLock;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.function.ThrowingRunnable;
import io.deephaven.util.function.ThrowingSupplier;
import io.deephaven.util.profiling.ThreadProfiler;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.minus;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.plus;

/**
 * Query performance instrumentation tools. Manages a hierarchy of {@link QueryPerformanceNugget} instances.
 */
public abstract class QueryPerformanceRecorder {

    public static final String UNINSTRUMENTED_CODE_DESCRIPTION = "Uninstrumented code";

    private static final String[] packageFilters;

    protected static final AtomicLong queriesProcessed = new AtomicLong(0);

    static final QueryPerformanceRecorder DUMMY_RECORDER = new DummyQueryPerformanceRecorder();

    /** thread local is package private to enable query resumption */
    static final ThreadLocal<QueryPerformanceRecorder> theLocal =
            ThreadLocal.withInitial(() -> DUMMY_RECORDER);
    private static final ThreadLocal<MutableLong> poolAllocatedBytes = ThreadLocal.withInitial(
            () -> new MutableLong(ThreadProfiler.DEFAULT.memoryProfilingAvailable() ? 0L
                    : io.deephaven.util.QueryConstants.NULL_LONG));
    private static final ThreadLocal<String> cachedCallsite = new ThreadLocal<>();

    static {
        final Configuration config = Configuration.getInstance();
        final Set<String> filters = new HashSet<>();

        final String propVal = config.getProperty("QueryPerformanceRecorder.packageFilter.internal");
        final URL path = QueryPerformanceRecorder.class.getResource("/" + propVal);
        if (path == null) {
            throw new RuntimeException("Can not locate package filter file " + propVal + " in classpath");
        }

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(path.openStream()))) {
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
        // clear interrupted - because this is a good place to do it - no cancellation exception here though
        // noinspection ResultOfMethodCallIgnored
        Thread.interrupted();
        theLocal.remove();
    }

    /**
     * @param name the nugget name
     * @return A new QueryPerformanceNugget to encapsulate user query operations. done() must be called on the nugget.
     */
    public abstract QueryPerformanceNugget getNugget(@NotNull String name);

    /**
     * @param name the nugget name
     * @param inputSize the nugget's input size
     * @return A new QueryPerformanceNugget to encapsulate user query operations. done() must be called on the nugget.
     */
    public abstract QueryPerformanceNugget getNugget(@NotNull String name, long inputSize);

    /**
     * @return The nugget currently in effect or else a dummy nugget if no nugget is in effect.
     */
    public abstract QueryPerformanceNugget getOuterNugget();

    /**
     * <b>Note:</b> Do not call this directly - it's for nugget use only. Call {@link QueryPerformanceNugget#done()} or
     * {@link QueryPerformanceNugget#close()} instead.
     *
     * @implNote This method is package private to limit visibility.
     * @param nugget the nugget to be released
     * @return If the nugget passes criteria for logging.
     */
    abstract boolean releaseNugget(QueryPerformanceNugget nugget);

    /**
     * @return the query level performance data
     */
    public abstract QueryPerformanceNugget getQueryLevelPerformanceData();

    /**
     * @return A list of loggable operation performance data.
     */
    public abstract List<QueryPerformanceNugget> getOperationLevelPerformanceData();

    public interface EntrySetter {
        void set(long evaluationNumber, int operationNumber, boolean uninstrumented);
    }

    /**
     * TODO NATE NOCOMMIT WRITE JAVADOC
     * 
     * @param setter
     */
    public abstract void setQueryData(final EntrySetter setter);

    /**
     * Install {@link QueryPerformanceRecorder#recordPoolAllocation(java.util.function.Supplier)} as the allocation
     * recorder for {@link io.deephaven.chunk.util.pools.ChunkPool chunk pools}.
     */
    public static void installPoolAllocationRecorder() {
        ChunkPoolInstrumentation.setAllocationRecorder(QueryPerformanceRecorder::recordPoolAllocation);
    }

    /**
     * Install this {@link QueryPerformanceRecorder} as the lock action recorder for {@link UpdateGraphLock}.
     */
    public static void installUpdateGraphLockInstrumentation() {
        UpdateGraphLock.installInstrumentation(new UpdateGraphLock.Instrumentation() {

            @Override
            public void recordAction(@NotNull String description, @NotNull Runnable action) {
                QueryPerformanceRecorder.withNugget(description, action);
            }

            @Override
            public void recordActionInterruptibly(@NotNull String description,
                    @NotNull ThrowingRunnable<InterruptedException> action)
                    throws InterruptedException {
                QueryPerformanceRecorder.withNuggetThrowing(description, action);
            }
        });
    }

    /**
     * Record a single-threaded operation's allocations as "pool" allocated memory attributable to the current thread.
     *
     * @param operation The operation to record allocation for
     * @return The result of the operation.
     */
    public static <RESULT_TYPE> RESULT_TYPE recordPoolAllocation(@NotNull final Supplier<RESULT_TYPE> operation) {
        final long startThreadAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
        try {
            return operation.get();
        } finally {
            final long endThreadAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
            final MutableLong poolAllocatedBytesForCurrentThread = poolAllocatedBytes.get();
            poolAllocatedBytesForCurrentThread.setValue(plus(poolAllocatedBytesForCurrentThread.longValue(),
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

                if (className.startsWith("io.deephaven.engine.util.GroovyDeephavenSession")) {
                    callerLineCandidate = "Groovy Script";
                } else if (Arrays.stream(packageFilters).noneMatch(className::startsWith)) {
                    callerLineCandidate = stack[i].getFileName() + ":" + stack[i].getLineNumber();
                }
            }
        }

        return callerLineCandidate == null ? "Internal" : callerLineCandidate;
    }

    /**
     * Surround the given code with a Performance Nugget
     *
     * @param name the nugget name
     * @param r the stuff to run
     */
    public static void withNugget(final String name, final Runnable r) {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;

        try {
            nugget = getInstance().getNugget(name);
            r.run();
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
    public static <T extends Exception> void withNuggetThrowing(
            final String name,
            final ThrowingRunnable<T> r) throws T {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;
        try {
            nugget = getInstance().getNugget(name);
            r.run();
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
    public static <R, ExceptionType extends Exception> R withNuggetThrowing(
            final String name,
            final ThrowingSupplier<R, ExceptionType> r) throws ExceptionType {
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
     * @param name the nugget name
     * @param r the stuff to run
     */
    public static void withNugget(final String name, final long inputSize, final Runnable r) {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;
        try {
            nugget = getInstance().getNugget(name, inputSize);
            r.run();
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
    public static <T extends Exception> void withNuggetThrowing(
            final String name,
            final long inputSize,
            final ThrowingRunnable<T> r) throws T {
        final boolean needClear = setCallsite();
        QueryPerformanceNugget nugget = null;
        try {
            nugget = getInstance().getNugget(name, inputSize);
            r.run();
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
    public static <R, ExceptionType extends Exception> R withNuggetThrowing(
            final String name,
            final long inputSize,
            final ThrowingSupplier<R, ExceptionType> r) throws ExceptionType {
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
     * <p>
     * Attempt to set the thread local callsite so that invocations of {@link #getCallerLine()} will not spend time
     * trying to recompute.
     * </p>
     *
     * <p>
     * This method returns a boolean if the value was successfully set. In the event this returns true, it's the
     * responsibility of the caller to invoke {@link #clearCallsite()} when the operation is complete.
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
     * Attempt to compute and set the thread local callsite so that invocations of {@link #getCallerLine()} will not
     * spend time trying to recompute.
     * </p>
     *
     * <p>
     * Users should follow the best practice as described by {@link #setCallsite(String)}
     * </p>
     *
     * @return true if the callsite was computed and set.
     */
    public static boolean setCallsite() {
        // This is very similar to the other getCallsite, but we don't want to invoke getCallerLine() unless we
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

    /**
     * Finish the nugget and clear the callsite if needed.
     *
     * @param nugget an optional nugget
     * @param needClear true if the callsite needs to be cleared
     */
    private static void finishAndClear(@Nullable final QueryPerformanceNugget nugget, final boolean needClear) {
        if (nugget != null) {
            nugget.done();
        }

        if (needClear) {
            clearCallsite();
        }
    }

    /**
     * Dummy recorder for use when no recorder is installed.
     */
    private static class DummyQueryPerformanceRecorder extends QueryPerformanceRecorder {

        @Override
        public QueryPerformanceNugget getNugget(@NotNull final String name) {
            return QueryPerformanceNugget.DUMMY_NUGGET;
        }

        @Override
        public QueryPerformanceNugget getNugget(@NotNull final String name, long inputSize) {
            return QueryPerformanceNugget.DUMMY_NUGGET;
        }

        @Override
        public QueryPerformanceNugget getOuterNugget() {
            return QueryPerformanceNugget.DUMMY_NUGGET;
        }

        @Override
        boolean releaseNugget(@NotNull final QueryPerformanceNugget nugget) {
            Assert.eqTrue(nugget == QueryPerformanceNugget.DUMMY_NUGGET,
                    "nugget == QueryPerformanceNugget.DUMMY_NUGGET");
            return false;
        }

        @Override
        public QueryPerformanceNugget getQueryLevelPerformanceData() {
            return QueryPerformanceNugget.DUMMY_NUGGET;
        }

        @Override
        public List<QueryPerformanceNugget> getOperationLevelPerformanceData() {
            return Collections.emptyList();
        }

        @Override
        public void setQueryData(EntrySetter setter) {
            setter.set(QueryConstants.NULL_LONG, QueryConstants.NULL_INT, false);
        }
    }
}
