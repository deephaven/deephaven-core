package io.deephaven.benchmarking;

import io.deephaven.db.v2.utils.metrics.MetricsManager;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Collection;

public class BenchUtil {
    public static final int defaultHeapGb = 4;

    public static void run(final Class c) {
        run(defaultHeapGb, c);
    }

    public static void run(final int heapGb, final Class c, String... benchmarkMethods) {
        final String[] regexes;
        if (benchmarkMethods.length == 0) {
            regexes = new String[] {c.getSimpleName()};
        } else {
            regexes = new String[benchmarkMethods.length];
            int i = 0;
            final String prefix = c.getSimpleName();
            for (String m : benchmarkMethods) {
                regexes[i++] = prefix + "." + m + "$";
            }
        }
        MetricsManager.instance.noPeriodicUpdates();
        final String heapGbStr = String.format("%dg", heapGb);
        final String[] jvmArgs = new String[] {
                "-Xms" + heapGbStr, "-Xmx" + heapGbStr,
                "-verbose:gc", "-XX:+PrintGCDetails",
                "-XX:+PrintGCTimeStamps", "-XX:+PrintGCDateStamps",
                "-XX:+PrintGCCause",
        };
        final OptionsBuilder builder = new OptionsBuilder();
        builder.jvmArgsPrepend(jvmArgs);
        for (String regex : regexes) {
            builder.include(regex);
        }
        final Options opt = builder.build();
        try {
            final Collection<RunResult> runResults = new Runner(opt).run();
            CsvResultWriter.recordResults(runResults, c);
        } catch (RunnerException e) {
            throw new RuntimeException(e);
        }
    }
}
