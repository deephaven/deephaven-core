package io.deephaven.db.v2.sort.timsort;

import io.deephaven.benchmarking.CsvResultWriter;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.*;
import java.util.stream.IntStream;

public class SortKernelBenchmark {
    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include("[A-Z].*SortKernelBenchmark")
                .warmupIterations(1)
                .measurementIterations(10)
                .measurementTime(TimeValue.seconds(1))
                .param("chunkSize",
                        IntStream.range(8, 21).mapToObj(exp -> Integer.toString(1 << exp)).toArray(String[]::new))
                .param("algorithm", "timsort", "javaarray")
                .param("runType", "random", "runs")
                .build();

        final Collection<RunResult> runResults = new Runner(opt).run();

        CsvResultWriter.recordResults(runResults, SortKernelBenchmark.class);
    }
}
