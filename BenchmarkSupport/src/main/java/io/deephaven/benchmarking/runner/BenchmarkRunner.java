package io.deephaven.benchmarking.runner;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.utils.TableBuilder;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.Utils;
import io.deephaven.benchmarking.BenchmarkTools;
import io.deephaven.benchmarking.ConcurrentResourceProfiler;
import io.deephaven.benchmarking.CsvResultWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkRunner {
    private static final TableDefinition RESULT_TABLE_DEF =
        BenchmarkTools.getLogDefinitionWithExtra(
            Arrays.asList(
                ColumnDefinition.ofInt("Run"),
                ColumnDefinition.ofDouble("Score"),
                ColumnDefinition.ofLong("TotalHeap"),
                ColumnDefinition.ofLong("FreeHeap"),
                ColumnDefinition.ofLong("UsedHeap"),
                ColumnDefinition.ofLong("Threads"),
                ColumnDefinition.ofDouble("CPULoad")));

    private static final int RETRY_LIMIT = 5;

    public static void main(String[] args) throws IOException {
        try {
            final Runner runner = new Runner(new OptionsBuilder()
                .parent(new CommandLineOptions(args))
                .addProfiler(ConcurrentResourceProfiler.class)
                .build());
            final Collection<RunResult> run = runner.run();
            recordResults(run);
            CsvResultWriter.recordResults(run,
                new File(BenchmarkTools.getLogPath() + File.separator + "Benchmark"));
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    private static void recordResults(Collection<RunResult> results) {
        if (results.isEmpty()) {
            return;
        }

        // Write an in mem table, load the per iteration tables, nj them, write to disk
        final TableBuilder builder = new TableBuilder(RESULT_TABLE_DEF);
        int runNo = 0;
        for (final RunResult runResult : results) {
            final BenchmarkParams runParams = runResult.getParams();
            final String paramString = BenchmarkTools.buildParameterString(runParams);
            final String benchmarkName = BenchmarkTools.getStrippedBenchmarkName(runParams);
            final String modeString = BenchmarkTools.getModeString(runParams);

            for (final BenchmarkResult benchResult : runResult.getBenchmarkResults()) {
                int itNo = 0;
                for (final IterationResult itResult : benchResult.getIterationResults()) {
                    builder.addRow(benchmarkName,
                        modeString,
                        itNo++,
                        paramString,
                        runNo,
                        filterDouble(itResult.getPrimaryResult().getScore()),
                        (long) (itResult.getSecondaryResults().get("Max heap").getScore()),
                        (long) (itResult.getSecondaryResults().get("Max free heap").getScore()),
                        (long) (itResult.getSecondaryResults().get("Max used heap").getScore()),
                        (long) (itResult.getSecondaryResults().get("Max threads").getScore()),
                        filterDouble(itResult.getSecondaryResults().get("Max CPU").getScore()));
                }
                runNo++;
            }
        }

        final Table topLevel = builder.build();
        final Table mergedDetails = getMergedDetails();
        final Table result =
            topLevel.naturalJoin(mergedDetails, "Benchmark,Mode,Run,Iteration,Params");

        final Path outputPath = Paths.get(BenchmarkTools.getLogPath())
            .resolve("Benchmark" + ParquetTableWriter.PARQUET_FILE_EXTENSION);

        ParquetTools.writeTable(result, outputPath.toFile(), result.getDefinition());
    }

    private static Table getMergedDetails() {
        final File[] files = FileUtils.missingSafeListFiles(
            new File(BenchmarkTools.getLogPath()),
            file -> file.getName().startsWith(BenchmarkTools.DETAIL_LOG_PREFIX));
        Arrays.sort(files, Utils.getModifiedTimeComparator(false));

        boolean OK;
        int retries;
        final Table[] detailTables = new Table[files.length];
        for (int i = 0; i < files.length; i++) {
            OK = false;
            retries = 0;
            while (!OK && retries < RETRY_LIMIT) {
                try {
                    detailTables[i] = TableBenchmarkState.readBin(files[i])
                        .update("Run=" + i);
                    OK = true;
                } catch (AssertionFailure af) {
                    retries++;
                }
            }

            if (!OK && (retries == RETRY_LIMIT)) {
                throw new RuntimeException("Failed to readBin " + files[i].getAbsolutePath()
                    + " after " + RETRY_LIMIT + " attempts.");
            }
            files[i].deleteOnExit();
        }

        return TableTools.merge(detailTables);
    }

    private static double filterDouble(double original) {
        if (Double.isNaN(original) || Double.isInfinite(original)) {
            return QueryConstants.NULL_DOUBLE;
        }

        return original;
    }
}
