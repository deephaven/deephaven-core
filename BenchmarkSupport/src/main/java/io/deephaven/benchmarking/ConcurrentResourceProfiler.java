package io.deephaven.benchmarking;

import io.deephaven.benchmarking.runner.ProfilerStats;
import io.deephaven.benchmarking.runner.StatsGatherer;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.ArrayList;
import java.util.Collection;

/*
 * Collects resource utilization stats using a separate thread and StatsGatherer class. Since CPU Load is averaged over
 * the last minute (by Java), this profiler doesn't provide much different CPU information than sampling in
 * afterIteration; but memory and thread stats may be more accurate with this approach.
 */
public class ConcurrentResourceProfiler implements InternalProfiler {

    private final ProfilerStats stats = new ProfilerStats();
    private Thread gatherer;

    @Override
    public String getDescription() {
        return "Compute resources profiler";
    }

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        gatherer = new Thread(new StatsGatherer(stats));
        gatherer.start();
    }

    @Override
    public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams,
            IterationResult result) {

        gatherer.interrupt();

        final Collection<ScalarResult> results = new ArrayList<>();
        results.add(new ScalarResult("Max heap", stats.totalHeap, "bytes", AggregationPolicy.MAX));
        results.add(new ScalarResult("Max free heap", stats.freeHeap, "bytes", AggregationPolicy.MAX));
        results.add(new ScalarResult("Max used heap", stats.usedHeap, "bytes", AggregationPolicy.MAX));
        results.add(new ScalarResult("Max threads", stats.activeThreads, "threads", AggregationPolicy.MAX));
        results.add(new ScalarResult("Max CPU", stats.cpuLoad, "percent", AggregationPolicy.MAX));

        return results;
    }
}
