/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.profiling.ThreadProfiler;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.minus;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.plus;

/**
 * A smaller entry that simply records usage data, meant for aggregating into the larger entry.
 */
public class BasePerformanceEntry implements LogOutputAppendable {
    private long intervalUsageNanos;

    private long intervalCpuNanos;
    private long intervalUserCpuNanos;

    private long intervalAllocatedBytes;
    private long intervalPoolAllocatedBytes;

    private long startTimeNanos;

    private long startCpuNanos;
    private long startUserCpuNanos;

    private long startAllocatedBytes;
    private long startPoolAllocatedBytes;

    public synchronized void onBaseEntryStart() {
        startAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
        startPoolAllocatedBytes = QueryPerformanceRecorder.getPoolAllocatedBytesForCurrentThread();

        startUserCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadUserTime();
        startCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadCpuTime();
        startTimeNanos = System.nanoTime();
    }

    public synchronized void onBaseEntryEnd() {
        intervalUserCpuNanos = plus(intervalUserCpuNanos,
                minus(ThreadProfiler.DEFAULT.getCurrentThreadUserTime(), startUserCpuNanos));
        intervalCpuNanos =
                plus(intervalCpuNanos, minus(ThreadProfiler.DEFAULT.getCurrentThreadCpuTime(), startCpuNanos));

        intervalUsageNanos += System.nanoTime() - startTimeNanos;

        intervalPoolAllocatedBytes = plus(intervalPoolAllocatedBytes,
                minus(QueryPerformanceRecorder.getPoolAllocatedBytesForCurrentThread(), startPoolAllocatedBytes));
        intervalAllocatedBytes = plus(intervalAllocatedBytes,
                minus(ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes(), startAllocatedBytes));

        startAllocatedBytes = 0;
        startPoolAllocatedBytes = 0;

        startUserCpuNanos = 0;
        startCpuNanos = 0;
        startTimeNanos = 0;
    }

    synchronized void baseEntryReset() {
        Assert.eqZero(startTimeNanos, "startTimeNanos");

        intervalUsageNanos = 0;

        intervalCpuNanos = 0;
        intervalUserCpuNanos = 0;

        intervalAllocatedBytes = 0;
        intervalPoolAllocatedBytes = 0;
    }

    /**
     * Get the aggregate usage in nanoseconds. Invoking this getter is valid iff the entry will no longer be mutated.
     *
     * @return total wall clock time in nanos
     */
    public long getTotalTimeNanos() {
        return intervalUsageNanos;
    }

    /**
     * Get the aggregate cpu time in nanoseconds. Invoking this getter is valid iff the entry will no longer be mutated.
     * 
     * @return total cpu time in nanos
     */
    public long getCpuNanos() {
        return intervalCpuNanos;
    }

    /**
     * Get the aggregate cpu user time in nanoseconds. Invoking this getter is valid iff the entry will no longer be
     * mutated.
     *
     * @return total cpu user time in nanos
     */
    public long getUserCpuNanos() {
        return intervalUserCpuNanos;
    }

    /**
     * Get the aggregate allocated memory in bytes. Invoking this getter is valid iff the entry will no longer be
     * mutated.
     *
     * @return The bytes of allocated memory attributed to the instrumented operation.
     */
    public long getAllocatedBytes() {
        return intervalAllocatedBytes;
    }

    /**
     * Get allocated pooled/reusable memory attributed to the instrumented operation in bytes. Invoking this getter is
     * valid iff the entry will no longer be mutated.
     *
     * @return total pool allocated memory in bytes
     */
    public long getPoolAllocatedBytes() {
        return intervalPoolAllocatedBytes;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        final LogOutput currentValues = logOutput.append("BasePerformanceEntry{")
                .append(", intervalUsageNanos=").append(intervalUsageNanos)
                .append(", intervalCpuNanos=").append(intervalCpuNanos)
                .append(", intervalUserCpuNanos=").append(intervalUserCpuNanos)
                .append(", intervalAllocatedBytes=").append(intervalAllocatedBytes)
                .append(", intervalPoolAllocatedBytes=").append(intervalPoolAllocatedBytes);
        return appendStart(currentValues)
                .append('}');
    }

    LogOutput appendStart(LogOutput logOutput) {
        return logOutput
                .append(", startCpuNanos=").append(startCpuNanos)
                .append(", startUserCpuNanos=").append(startUserCpuNanos)
                .append(", startTimeNanos=").append(startTimeNanos)
                .append(", startAllocatedBytes=").append(startAllocatedBytes)
                .append(", startPoolAllocatedBytes=").append(startPoolAllocatedBytes);
    }

    /**
     * Accumulate the values from another entry into this one. The provided entry will not be mutated.
     *
     * @param entry the entry to accumulate
     */
    public synchronized void accumulate(@NotNull final BasePerformanceEntry entry) {
        this.intervalUsageNanos += entry.intervalUsageNanos;
        this.intervalCpuNanos = plus(this.intervalCpuNanos, entry.intervalCpuNanos);
        this.intervalUserCpuNanos = plus(this.intervalUserCpuNanos, entry.intervalUserCpuNanos);

        this.intervalAllocatedBytes = plus(this.intervalAllocatedBytes, entry.intervalAllocatedBytes);
        this.intervalPoolAllocatedBytes = plus(this.intervalPoolAllocatedBytes, entry.intervalPoolAllocatedBytes);
    }
}
