package io.deephaven.benchmarking;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.*;

import java.util.ArrayList;
import java.util.Collection;

/*
 * Collects resource utilization stats once, at the end of the iteration. Since CPU Load is averaged over the last
 * minute (by Java), this profiler doesn't provide much different CPU information than sampling concurrently on another
 * thread as is done in the ConcurrentResourceProfiler.
 */
public class ResourceProfiler implements InternalProfiler {

    @Override
    public String getDescription() {
        return "Compute resources profiler";
    }

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {

    }

    @Override
    public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams,
            IterationResult result) {

        final long totalHeap = Runtime.getRuntime().totalMemory();
        final long freeHeap = Runtime.getRuntime().freeMemory();
        final long usedHeap = java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        final long activeThreads = java.lang.Thread.activeCount();
        final double cpuLoad = java.lang.management.ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage()
                /
                java.lang.management.ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors() * 100.0;

        final Collection<ScalarResult> results = new ArrayList<>();
        results.add(new ScalarResult("Max heap", totalHeap, "bytes", AggregationPolicy.MAX));
        results.add(new ScalarResult("Max free heap", freeHeap, "bytes", AggregationPolicy.MAX));
        results.add(new ScalarResult("Max used heap", usedHeap, "bytes", AggregationPolicy.MAX));
        results.add(new ScalarResult("Max threads", activeThreads, "threads", AggregationPolicy.MAX));
        results.add(new ScalarResult("Max CPU", cpuLoad, "percent", AggregationPolicy.MAX));

        return results;
    }
}
