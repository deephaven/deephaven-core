package io.deephaven.benchmark.db;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.Table;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@Timeout(time = 3)
@Fork(1)
public class SingleTableKeyedStringOperations {
    private TableBenchmarkState state;
    BenchmarkTable bmTable;

    // @Param({"0", "1", "2", "4"})
    @Param({"0", "1"})
    private int logColumnCount;

    // @Param({"100", "10000", "1000000" /*, "100000000" */})
    @Param({"1000000"})
    private int tableSize;

    @Param({"100", /* "90", "50", */ "10"}) // , "10", "5", "1"})
    private int sparsity;


    // @Param({"0", "4", "8","16","31"})
    @Param({"4", "16", "31"})
    private int logSpaceSize;

    private Table inputTable;

    private String[] keyColumns;
    String[] convertToDateTime;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        int columnCount = 1 << logColumnCount;
        Configuration.getInstance().setProperty("QueryTable.memoizeResults", "false");

        final BenchmarkTableBuilder builder = BenchmarkTools.inMemoryTableBuilder("SingleTableOperations",
                BenchmarkTools.sizeWithSparsity(tableSize, sparsity));

        builder.setSeed(0xDEADBEEF).addColumn(BenchmarkTools.numberCol("Mock", int.class));
        for (int i = 0; i < columnCount; i++) {
            builder.addColumn(BenchmarkTools.numberCol("InputColumn" + i, long.class, 0,
                    1 << Math.max(0, logSpaceSize - logColumnCount)));
        }
        bmTable = builder.build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());


        keyColumns = new String[columnCount];
        convertToDateTime = new String[columnCount + 1];
        for (int i = 0; i < keyColumns.length; i++) {
            keyColumns[i] = "InputColumn" + i;
            convertToDateTime[i] = keyColumns[i] + " = ``+" + keyColumns[i];
        }
        convertToDateTime[columnCount] = "Mock";
    }


    @Setup(Level.Iteration)
    public void setupIteration() {
        state.init();
        inputTable = applySparsity(bmTable.getTable().select(convertToDateTime), tableSize, sparsity, 0);
    }


    @Benchmark
    public Table by() {
        Table result = inputTable.by(keyColumns);
        return result;
    }

    @Benchmark
    public Table lastBy() {
        return inputTable.lastBy(keyColumns);
    }

    public static void main(String[] args) {
        BenchUtil.run(SingleTableKeyedStringOperations.class);
    }
}
