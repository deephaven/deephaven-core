package io.deephaven.benchmark.db;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.Collection;
import java.util.Collections;

/**
 * Simple profiler that just records how many rows were in the result (presuming you set the result
 * size).
 */
public class ResultSizeProfiler implements InternalProfiler {
    private static long resultSize;

    @Override
    public String getDescription() {
        return "Result Size";
    }

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        resultSize = -1;
    }

    @Override
    public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams,
        IterationParams iterationParams,
        IterationResult result) {
        return Collections
            .singleton(new ScalarResult("Result size", resultSize, "rows", AggregationPolicy.AVG));
    }

    public static void setResultSize(long resultSize) {
        ResultSizeProfiler.resultSize = resultSize;
    }
}
