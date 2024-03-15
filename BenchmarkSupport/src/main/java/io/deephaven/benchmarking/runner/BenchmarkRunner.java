//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmarking.runner;

import io.deephaven.benchmarking.BenchmarkTools;
import io.deephaven.benchmarking.ConcurrentResourceProfiler;
import io.deephaven.benchmarking.CsvResultWriter;
import java.io.IOException;
import java.util.Collection;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkRunner {

    public static void main(String[] args) throws IOException {
        try {
            final Runner runner = new Runner(new OptionsBuilder()
                    .parent(new CommandLineOptions(args))
                    .addProfiler(ConcurrentResourceProfiler.class)
                    .build());
            final Collection<RunResult> run = runner.run();
            CsvResultWriter.recordResults(run, BenchmarkTools.dataDir().resolve("Benchmark").toFile());
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

}
