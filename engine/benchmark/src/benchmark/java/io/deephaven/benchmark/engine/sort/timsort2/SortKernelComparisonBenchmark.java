//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sort.timsort2;

import io.deephaven.benchmarking.CsvResultWriter;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.*;

/**
 * Compares the replicated timsort kernels (io.deephaven.engine.table.impl.sort.timsort, via the benchmarks in
 * io.deephaven.benchmark.engine.sort.timsort) with the JavaPoet-generated kernels
 * (io.deephaven.engine.table.impl.sort.timsort2, via the benchmarks in this package). The include pattern matches the
 * per-type benchmark classes in both packages.
 */
public class SortKernelComparisonBenchmark {
    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include("sort\\.timsort2?\\.[A-Z].*SortKernelBenchmark")
                .warmupIterations(2)
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(1))
                .param("chunkSize", "1024", "65536", "1048576")
                .param("algorithm", "timsort")
                .param("runType", "random", "runs")
                .build();

        final Collection<RunResult> runResults = new Runner(opt).run();

        CsvResultWriter.recordResults(runResults, SortKernelComparisonBenchmark.class);
    }
}
