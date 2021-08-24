package io.deephaven.benchmarking;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.RunResult;

import java.io.File;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class CsvResultWriter {
    public static final String TEST_OUTPUT_DIR_PATH =
        System.getProperty("test.output.dir", "tmp" + File.separator + "logs");

    public static void recordResults(final Collection<RunResult> results, final Class c) {
        final String className = c.getSimpleName();
        final String timeString =
            new SimpleDateFormat("yyyy-MM-dd-HHmmss").format(new Date(System.currentTimeMillis()));
        recordResults(results, new File(
            TEST_OUTPUT_DIR_PATH + File.separator + className + "-" + timeString + ".csv"));
    }

    public static void recordResults(final Collection<RunResult> results, final File file) {
        if (results.isEmpty()) {
            return;
        }

        final CsvWriter writer = new CsvWriter(file, new CsvWriterSettings());

        final Set<String> headers = new LinkedHashSet<>();

        headers.add("Benchmark");
        headers.add("Run");
        headers.add("Iteration");
        headers.add("Score");

        for (final RunResult runResult : results) {
            final BenchmarkParams runParams = runResult.getParams();
            headers.addAll(runParams.getParamsKeys());
        }

        writer.writeHeaders(headers);

        final DecimalFormat decimalFormat = new DecimalFormat("#0.000");

        int runNo = 0;
        for (final RunResult runResult : results) {
            final BenchmarkParams runParams = runResult.getParams();

            for (final BenchmarkResult benchResult : runResult.getBenchmarkResults()) {
                runNo++;
                int itNo = 0;
                for (final IterationResult itResult : benchResult.getIterationResults()) {
                    itNo++;
                    final Map<String, String> values = new HashMap<>();
                    values.put("Benchmark", runParams.getBenchmark());
                    for (String key : runParams.getParamsKeys()) {
                        values.put(key, runParams.getParam(key));
                    }
                    values.put("Score",
                        decimalFormat.format(itResult.getPrimaryResult().getScore()));
                    values.put("Run", Integer.toString(runNo));
                    values.put("Iteration", Integer.toString(itNo));

                    writer.writeRow(headers.stream().map(values::get).collect(Collectors.toList()));
                }
            }
        }

        writer.close();
    }
}
