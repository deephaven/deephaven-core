package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.profiling.ThreadProfiler;

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

    public void onBaseEntryStart() {
        startAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
        startPoolAllocatedBytes = QueryPerformanceRecorder.getPoolAllocatedBytesForCurrentThread();

        startUserCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadUserTime();
        startCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadCpuTime();
        startTimeNanos = System.nanoTime();
    }

    public void onBaseEntryEnd() {
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

    void baseEntryReset() {
        Assert.eqZero(startTimeNanos, "startTimeNanos");

        intervalUsageNanos = 0;

        intervalCpuNanos = 0;
        intervalUserCpuNanos = 0;

        intervalAllocatedBytes = 0;
        intervalPoolAllocatedBytes = 0;
    }

    public long getIntervalUsageNanos() {
        return intervalUsageNanos;
    }

    public long getIntervalCpuNanos() {
        return intervalCpuNanos;
    }

    public long getIntervalUserCpuNanos() {
        return intervalUserCpuNanos;
    }

    public long getIntervalAllocatedBytes() {
        return intervalAllocatedBytes;
    }

    public long getIntervalPoolAllocatedBytes() {
        return intervalPoolAllocatedBytes;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
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

    public void accumulate(BasePerformanceEntry entry) {
        this.intervalUsageNanos += entry.intervalUsageNanos;
        this.intervalCpuNanos = plus(this.intervalCpuNanos, entry.intervalCpuNanos);
        this.intervalUserCpuNanos = plus(this.intervalUserCpuNanos, entry.intervalUserCpuNanos);

        this.intervalAllocatedBytes = plus(this.intervalAllocatedBytes, entry.intervalAllocatedBytes);
        this.intervalPoolAllocatedBytes = plus(this.intervalPoolAllocatedBytes, entry.intervalPoolAllocatedBytes);
    }
}
