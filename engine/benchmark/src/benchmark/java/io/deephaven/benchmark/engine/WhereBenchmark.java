/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTools;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.generator.EnumStringGenerator;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@Timeout(time = 3)
@Fork(1)
public class WhereBenchmark {
    private WhereFilter whereFilter;
    private TableBenchmarkState state;
    BenchmarkTable bmTable;

    @Param({"StringGroupedMatch",
            "StringGroupedCondition",
            "StringUngroupedCondition",
            "DoubleUngroupedCondition",
            "DoubleTwoUngroupedCondition"})
    private String testType;

    @Param({"Historical", "Intraday"})
    private String tableType;

    @Param({"100", "10000", "1000000"})
    private int tableSize;

    @Param({"100", "90", "50", "20"}) // , "10", "5", "1"})
    private int sparsity;
    private Table inputTable;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        final EnumStringGenerator generator = new EnumStringGenerator(
                30, 0xB00FB00FL, EnumStringGenerator.Mode.Random, 6, 6);
        final ColumnGenerator<String> enumStringyCol = BenchmarkTools.stringCol("Thingy", generator);

        final BenchmarkTableBuilder builder;
        switch (tableType) {
            case "Historical":
                builder = BenchmarkTools
                        .persistentTableBuilder("Carlos", BenchmarkTools.sizeWithSparsity(tableSize, sparsity))
                        .addGroupingColumns("Thingy")
                        .setPartitioningFormula("${autobalance_single}")
                        .setPartitionCount(10);
                break;
            case "Intraday":
                builder = BenchmarkTools.persistentTableBuilder("Carlos",
                        BenchmarkTools.sizeWithSparsity(tableSize, sparsity));
                break;

            default:
                throw new IllegalStateException("Table type must be Historical or Intraday");
        }

        bmTable = builder
                .setSeed(0xDEADBEEF)
                .addColumn(BenchmarkTools.stringCol("Stringy", 1, 10))
                .addColumn(BenchmarkTools.numberCol("C2", int.class))
                .addColumn(BenchmarkTools.numberCol("C3", double.class, -10e6, 10e6))
                .addColumn(BenchmarkTools.stringCol("C4", 4, 5, 7, 0xFEEDBEEF))
                .addColumn(BenchmarkTools.numberCol("C5", double.class, -10e6, 10e6))
                .addColumn(enumStringyCol)
                .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());

        final List<String> uniqueThingyVals = Arrays.asList(generator.getEnumVals());
        final String filterString;

        switch (testType) {
            case "StringGroupedMatch":
                filterString = "Thingy in " +
                        "`" + uniqueThingyVals.get(0) + "`, " +
                        "`" + uniqueThingyVals.get(uniqueThingyVals.size() - 1) + "`, " +
                        "`NotInTheSet`";
                break;
            case "StringGroupedCondition":
                filterString = "Thingy.startsWith(`" + uniqueThingyVals.get(0).substring(0, 2) + "`)";
                break;
            case "DoubleUngroupedCondition":
                filterString = "C3 > 0";
                break;

            case "DoubleTwoUngroupedCondition":
                filterString = "C3 > C5";
                break;

            case "StringUngroupedCondition":
                filterString = "Stringy.contains(`X`)";
                break;

            default:
                throw new IllegalStateException("Can't touch this.");
        }

        whereFilter = WhereFilterFactory.getExpression(filterString);
    }

    @TearDown(Level.Trial)
    public void finishTrial() {
        try {
            state.logOutput();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        state.init();
        inputTable = applySparsity(bmTable.getTable(), tableSize, sparsity, 0);
    }

    @TearDown(Level.Iteration)
    public void finishIteration(BenchmarkParams params) throws IOException {
        state.processResult(params);
    }

    @Benchmark
    public Table where() {
        return state.setResult(inputTable.where(whereFilter)).coalesce();
    }

    public static void main(String[] args) {
        BenchUtil.run(WhereBenchmark.class);
    }
}
