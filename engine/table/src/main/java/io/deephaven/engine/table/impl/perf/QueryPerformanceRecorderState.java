//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.chunk.util.pools.ChunkPoolInstrumentation;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.updategraph.UpdateGraphLock;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.function.ThrowingRunnable;
import io.deephaven.util.profiling.ThreadProfiler;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.minus;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.plus;

public abstract class QueryPerformanceRecorderState {

    static final QueryPerformanceRecorder DUMMY_RECORDER = new DummyQueryPerformanceRecorder();
    static final AtomicLong QUERIES_PROCESSED = new AtomicLong(0);
    static final ThreadLocal<QueryPerformanceRecorder> THE_LOCAL = ThreadLocal.withInitial(() -> DUMMY_RECORDER);

    private static final String[] PACKAGE_FILTERS;
    private static final ThreadLocal<String> CACHED_CALLSITE = new ThreadLocal<>();
    private static final ThreadLocal<MutableLong> POOL_ALLOCATED_BYTES = ThreadLocal.withInitial(
            () -> new MutableLong(ThreadProfiler.DEFAULT.memoryProfilingAvailable()
                    ? 0L
                    : io.deephaven.util.QueryConstants.NULL_LONG));

    static {
        // initialize the packages to skip when determining the callsite

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

        PACKAGE_FILTERS = filters.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    }

    private QueryPerformanceRecorderState() {
        throw new UnsupportedOperationException("static use only");
    }

    public static QueryPerformanceRecorder getInstance() {
        return THE_LOCAL.get();
    }

    static void resetInstance() {
        // clear interrupted - because this is a good place to do it - no cancellation exception here though
        // noinspection ResultOfMethodCallIgnored
        Thread.interrupted();
        THE_LOCAL.remove();
    }

    /**
     * Install {@link QueryPerformanceRecorderState#recordPoolAllocation(java.util.function.Supplier)} as the allocation
     * recorder for {@link io.deephaven.chunk.util.pools.ChunkPool chunk pools}.
     */
    public static void installPoolAllocationRecorder() {
        ChunkPoolInstrumentation.setAllocationRecorder(QueryPerformanceRecorderState::recordPoolAllocation);
    }

    /**
     * Use nuggets from the current {@link QueryPerformanceRecorder} as the lock action recorder for
     * {@link UpdateGraphLock}.
     */
    public static void installUpdateGraphLockInstrumentation() {
        UpdateGraphLock.installInstrumentation(new UpdateGraphLock.Instrumentation() {

            @Override
            public void recordAction(@NotNull final String description, @NotNull final Runnable action) {
                QueryPerformanceRecorder.withNugget(description, action);
            }

            @Override
            public void recordActionInterruptibly(
                    @NotNull final String description,
                    @NotNull final ThrowingRunnable<InterruptedException> action) throws InterruptedException {
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
    static <RESULT_TYPE> RESULT_TYPE recordPoolAllocation(@NotNull final Supplier<RESULT_TYPE> operation) {
        final long startThreadAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
        try {
            return operation.get();
        } finally {
            final long endThreadAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
            final MutableLong poolAllocatedBytesForCurrentThread = POOL_ALLOCATED_BYTES.get();
            poolAllocatedBytesForCurrentThread.set(plus(poolAllocatedBytesForCurrentThread.get(),
                    minus(endThreadAllocatedBytes, startThreadAllocatedBytes)));
        }
    }

    /**
     * Get the total bytes of pool-allocated memory attributed to this thread via
     * {@link #recordPoolAllocation(Supplier)}.
     *
     * @return The total bytes of pool-allocated memory attributed to this thread.
     */
    static long getPoolAllocatedBytesForCurrentThread() {
        return POOL_ALLOCATED_BYTES.get().get();
    }

    /**
     * See {@link QueryPerformanceRecorder#getCallerLine()}.
     */
    static String getCallerLine() {
        String callerLineCandidate = CACHED_CALLSITE.get();

        if (callerLineCandidate == null) {
            final StackTraceElement[] stack = (new Exception()).getStackTrace();
            for (int i = stack.length - 1; i > 0; i--) {
                final String className = stack[i].getClassName();

                if (className.startsWith("io.deephaven.engine.util.GroovyDeephavenSession")) {
                    callerLineCandidate = "Groovy Script";
                } else if (Arrays.stream(PACKAGE_FILTERS).noneMatch(className::startsWith)) {
                    callerLineCandidate = stack[i].getFileName() + ":" + stack[i].getLineNumber();
                }
            }
        }

        return callerLineCandidate == null ? "Internal" : callerLineCandidate;
    }

    /**
     * See {@link QueryPerformanceRecorder#setCallsite(String)}.
     */
    static boolean setCallsite(String callsite) {
        if (CACHED_CALLSITE.get() == null) {
            CACHED_CALLSITE.set(callsite);
            return true;
        }

        return false;
    }

    /**
     * See {@link QueryPerformanceRecorder#setCallsite()}.
     */
    static boolean setCallsite() {
        // This is very similar to the other setCallsite overload, but we don't want to invoke getCallerLine() unless we
        // really need to.
        if (CACHED_CALLSITE.get() == null) {
            CACHED_CALLSITE.set(getCallerLine());
            return true;
        }

        return false;
    }

    /**
     * Clear any previously set callsite. See {@link #setCallsite(String)}
     */
    static void clearCallsite() {
        CACHED_CALLSITE.remove();
    }

    /**
     * Dummy recorder for use when no recorder is installed.
     */
    private static class DummyQueryPerformanceRecorder implements QueryPerformanceRecorder {

        @Override
        public QueryPerformanceNugget getNugget(@NotNull final String name, long inputSize) {
            return QueryPerformanceNugget.DUMMY_NUGGET;
        }

        @Override
        public QueryPerformanceNugget getCompilationNugget(@NotNull final String name) {
            return QueryPerformanceNugget.DUMMY_NUGGET;
        }

        @Override
        public QueryPerformanceNugget getEnclosingNugget() {
            return QueryPerformanceNugget.DUMMY_NUGGET;
        }

        @Override
        public void supplyQueryData(final @NotNull QueryDataConsumer consumer) {
            consumer.accept(QueryConstants.NULL_LONG, QueryConstants.NULL_INT, false);
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
        public void accumulate(@NotNull QueryPerformanceRecorder subQuery) {
            // no-op
        }

        @Override
        public boolean hasSubQueries() {
            return false;
        }

        @Override
        public QueryState getState() {
            throw new UnsupportedOperationException("Dummy recorder does not support getState()");
        }

        @Override
        public SafeCloseable startQuery() {
            throw new UnsupportedOperationException("Dummy recorder does not support startQuery()");
        }

        @Override
        public boolean endQuery() {
            throw new UnsupportedOperationException("Dummy recorder does not support endQuery()");
        }

        @Override
        public void suspendQuery() {
            throw new UnsupportedOperationException("Dummy recorder does not support suspendQuery()");
        }

        @Override
        public SafeCloseable resumeQuery() {
            throw new UnsupportedOperationException("Dummy recorder does not support resumeQuery()");
        }

        @Override
        public void abortQuery() {
            throw new UnsupportedOperationException("Dummy recorder does not support abortQuery()");
        }
    }
}
