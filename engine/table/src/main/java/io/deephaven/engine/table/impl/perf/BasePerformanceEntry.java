//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
    private long usageNanos;

    private long cpuNanos;
    private long userCpuNanos;

    private long allocatedBytes;
    private long poolAllocatedBytes;

    private long startTimeNanos;

    private long startCpuNanos;
    private long startUserCpuNanos;

    private long startAllocatedBytes;
    private long startPoolAllocatedBytes;

    public synchronized void onBaseEntryStart() {
        startAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
        startPoolAllocatedBytes = QueryPerformanceRecorderState.getPoolAllocatedBytesForCurrentThread();

        startUserCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadUserTime();
        startCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadCpuTime();
        startTimeNanos = System.nanoTime();
    }

    public synchronized void onBaseEntryEnd() {
        userCpuNanos = plus(userCpuNanos,
                minus(ThreadProfiler.DEFAULT.getCurrentThreadUserTime(), startUserCpuNanos));
        cpuNanos =
                plus(cpuNanos, minus(ThreadProfiler.DEFAULT.getCurrentThreadCpuTime(), startCpuNanos));

        usageNanos += System.nanoTime() - startTimeNanos;

        poolAllocatedBytes = plus(poolAllocatedBytes,
                minus(QueryPerformanceRecorderState.getPoolAllocatedBytesForCurrentThread(), startPoolAllocatedBytes));
        allocatedBytes = plus(allocatedBytes,
                minus(ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes(), startAllocatedBytes));

        startAllocatedBytes = 0;
        startPoolAllocatedBytes = 0;

        startUserCpuNanos = 0;
        startCpuNanos = 0;
        startTimeNanos = 0;
    }

    synchronized void baseEntryReset() {
        Assert.eqZero(startTimeNanos, "startTimeNanos");

        usageNanos = 0;

        cpuNanos = 0;
        userCpuNanos = 0;

        allocatedBytes = 0;
        poolAllocatedBytes = 0;
    }

    /**
     * Get the aggregate usage in nanoseconds. This getter should be called by exclusive owners of the entry, and never
     * concurrently with mutators.
     *
     * @return total wall clock time in nanos
     */
    public long getUsageNanos() {
        return usageNanos;
    }

    /**
     * Get the aggregate cpu time in nanoseconds. This getter should be called by exclusive owners of the entry, and
     * never concurrently with mutators.
     *
     * @return total cpu time in nanos
     */
    public long getCpuNanos() {
        return cpuNanos;
    }

    /**
     * Get the aggregate cpu user time in nanoseconds. This getter should be called by exclusive owners of the entry,
     * and never concurrently with mutators.
     *
     * @return total cpu user time in nanos
     */
    public long getUserCpuNanos() {
        return userCpuNanos;
    }

    /**
     * Get the aggregate allocated memory in bytes. This getter should be called by exclusive owners of the entry, and
     * never concurrently with mutators.
     *
     * @return The bytes of allocated memory attributed to the instrumented operation.
     */
    public long getAllocatedBytes() {
        return allocatedBytes;
    }

    /**
     * Get allocated pooled/reusable memory attributed to the instrumented operation in bytes. This getter should be
     * called by exclusive owners of the entry, and never concurrently with mutators.
     *
     * @return total pool allocated memory in bytes
     */
    public long getPoolAllocatedBytes() {
        return poolAllocatedBytes;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        final LogOutput currentValues = logOutput.append("BasePerformanceEntry{")
                .append(", intervalUsageNanos=").append(usageNanos)
                .append(", intervalCpuNanos=").append(cpuNanos)
                .append(", intervalUserCpuNanos=").append(userCpuNanos)
                .append(", intervalAllocatedBytes=").append(allocatedBytes)
                .append(", intervalPoolAllocatedBytes=").append(poolAllocatedBytes);
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
        this.usageNanos += entry.usageNanos;
        this.cpuNanos = plus(this.cpuNanos, entry.cpuNanos);
        this.userCpuNanos = plus(this.userCpuNanos, entry.userCpuNanos);

        this.allocatedBytes = plus(this.allocatedBytes, entry.allocatedBytes);
        this.poolAllocatedBytes = plus(this.poolAllocatedBytes, entry.poolAllocatedBytes);
    }
}
