package io.deephaven.benchmarking.runner;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.utils.TableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class TableBenchmarkState {
    private static final TableDefinition RESULT_DEF = BenchmarkTools.getLogDefinitionWithExtra(
        Collections.singletonList(ColumnDefinition.ofString("Fingerprint")));

    private final String benchmarkName;
    private final TableBuilder outputBuilder;
    private final int expectedWarmups;

    private Table resultTable;
    private int iteration = 0;
    private int warmups = 0;


    public TableBenchmarkState(String benchmarkName, int expectedWarmups) {
        this.benchmarkName = benchmarkName;
        this.outputBuilder = new TableBuilder(RESULT_DEF, 1024);
        this.expectedWarmups = expectedWarmups;
    }

    public void init() {
        reset();
    }

    public void logOutput() throws IOException {
        final Path outputPath = Paths.get(BenchmarkTools.getLogPath())
            .resolve(BenchmarkTools.getDetailOutputPath(benchmarkName)
                + ParquetTableWriter.PARQUET_FILE_EXTENSION);

        final Table output = outputBuilder.build();
        ParquetTools.writeTable(output, outputPath.toFile(), RESULT_DEF);
    }

    public void reset() {
        resultTable = null;
    }

    public void processResult(BenchmarkParams params) throws IOException {
        if (warmups < expectedWarmups) {
            warmups++;
            return;
        }

        outputBuilder.addRow(benchmarkName, params.getMode().toString(), iteration++,
            BenchmarkTools.buildParameterString(params),
            TableTools.base64Fingerprint(resultTable));
    }

    public Table setResult(Table result) {
        return this.resultTable = result;
    }

    public long resultSize() {
        return resultTable.size();
    }

    static Table readBin(File location) {
        return ParquetTools.readTable(location);
    }
}
