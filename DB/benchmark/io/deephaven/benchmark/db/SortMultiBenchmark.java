package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.generator.EnumStringColumnGenerator;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 0, time = 1)
@Measurement(iterations = 3, time = 1)
@Timeout(time = 3)
@Fork(1)
public class SortMultiBenchmark {
    private TableBenchmarkState state;
    private BenchmarkTable bmTable;

    @Param({"Enum1,D1",
            "Enum1,L1",
            "Enum1,I1",
            "I1,Enum2",
            "Enum1,Enum2"})
    private String sortCols;

    @Param({"Intraday"})
    private String tableType;

    @Param({"25000000"})
    private int tableSize;

    @Param({"90"}) // , "10", "5", "1"})
    private int sparsity;
    private Table inputTable;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        final EnumStringColumnGenerator enumStringCol1 =
            (EnumStringColumnGenerator) BenchmarkTools.stringCol("Enum1", 10000, 6, 6, 0xB00FB00F);
        final EnumStringColumnGenerator enumStringCol2 =
            (EnumStringColumnGenerator) BenchmarkTools.stringCol("Enum2", 1000, 6, 6, 0xF00DF00D);

        final BenchmarkTableBuilder builder;
        final int actualSize = BenchmarkTools.sizeWithSparsity(tableSize, sparsity);

        System.out.println("Actual Size: " + actualSize);

        switch (tableType) {
            case "Historical":
                builder = BenchmarkTools.persistentTableBuilder("Carlos", actualSize)
                    .addGroupingColumns("Enum1")
                    .setPartitioningFormula("${autobalance_single}")
                    .setPartitionCount(10);
                break;
            case "Intraday":
                builder = BenchmarkTools.persistentTableBuilder("Carlos", actualSize);
                break;

            default:
                throw new IllegalStateException("Table type must be Historical or Intraday");
        }

        bmTable = builder
            .setSeed(0xDEADBEEF)
            .addColumn(BenchmarkTools.stringCol("PartCol", 4, 5, 7, 0xFEEDBEEF))
            .addColumn(BenchmarkTools.numberCol("I1", int.class))
            .addColumn(BenchmarkTools.numberCol("D1", double.class, -10e6, 10e6))
            .addColumn(BenchmarkTools.numberCol("L1", long.class))
            .addColumn(enumStringCol1)
            .addColumn(enumStringCol2)
            .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()),
            params.getWarmup().getCount());
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
    public Table sort() {
        return state.setResult(inputTable.sort(sortCols.split(","))).coalesce();
    }

    public static void main(final String[] args) {
        BenchUtil.run(SortMultiBenchmark.class);
    }
}
