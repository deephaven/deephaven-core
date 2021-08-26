package io.deephaven.db.v2.sort.partition;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.*;
import java.util.stream.IntStream;

import io.deephaven.benchmarking.CsvResultWriter;

public class PartitionKernelBenchmark {
    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                // .include("[A-Z].*PartitionKernelBenchmark")
                .include("(Long|Int|Double|Object)PartitionKernelBenchmark")
                .warmupIterations(1)
                .measurementIterations(10)
                .measurementTime(TimeValue.seconds(1))
                // .param("dataSize", IntStream.range(8, 21).mapToObj(exp -> Integer.toString(1 <<
                // exp)).toArray(String[]::new))
                // .param("chunkSize", IntStream.range(10, 11).mapToObj(exp -> Integer.toString(1 <<
                // exp)).toArray(String[]::new))
                .param("dataSize",
                        IntStream.range(20, 24).mapToObj(exp -> Integer.toString(1 << exp)).toArray(String[]::new))
                .param("chunkSize",
                        IntStream.range(10, 17).mapToObj(exp -> Integer.toString(1 << exp)).toArray(String[]::new))
                .param("runType", "random")
                .param("preserveEquality", "true", "false")
                .jvmArgsPrepend("-Xmx8g")
                .build();

        final Collection<RunResult> runResults = new Runner(opt).run();

        CsvResultWriter.recordResults(runResults, PartitionKernelBenchmark.class);
    }
}
