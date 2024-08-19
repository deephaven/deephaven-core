//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmarking.runner;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.util.TableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import org.openjdk.jmh.infra.BenchmarkParams;

import static io.deephaven.parquet.base.ParquetUtils.PARQUET_FILE_EXTENSION;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

public class TableBenchmarkState {
    private static final TableDefinition RESULT_DEF = BenchmarkTools
            .getLogDefinitionWithExtra(Collections.singletonList(ColumnDefinition.ofString("Fingerprint")));

    private final String benchmarkName;
    private final TableBuilder outputBuilder;
    private final int expectedWarmups;

    private Table resultTable;
    private int iteration = 0;
    private int warmups = 0;
    private String base64ResultPrint = "";


    public TableBenchmarkState(String benchmarkName, int expectedWarmups) {
        this.benchmarkName = benchmarkName;
        this.outputBuilder = new TableBuilder(RESULT_DEF, 1024);
        this.expectedWarmups = expectedWarmups;
    }

    public void init() {
        reset();
    }

    public void logOutput() throws IOException {
        final Path outputPath = BenchmarkTools.dataDir()
                .resolve(BenchmarkTools.getDetailOutputPath(benchmarkName) + PARQUET_FILE_EXTENSION);

        final Table output = outputBuilder.build();
        ParquetTools.writeTable(output, outputPath.toString(),
                ParquetInstructions.EMPTY.withTableDefinition(RESULT_DEF));
    }

    public void reset() {
        resultTable = null;
        base64ResultPrint = "";
    }

    public void processResult(BenchmarkParams params) throws IOException {
        if (warmups < expectedWarmups) {
            warmups++;
            return;
        }

        outputBuilder.addRow(benchmarkName, params.getMode().toString(), iteration++,
                BenchmarkTools.buildParameterString(params),
                base64ResultPrint = TableTools.base64Fingerprint(resultTable));
    }

    public Table setResult(Table result) {
        return this.resultTable = result;
    }

    public long resultSize() {
        return resultTable.size();
    }

    public static Table readBin(File location) {
        return ParquetTools.readTable(location.getPath());
    }

    public String getResultHash() {
        return base64ResultPrint;
    }
}
