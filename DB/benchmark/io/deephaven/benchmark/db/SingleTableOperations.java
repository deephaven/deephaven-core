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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@Timeout(time = 3)
@Fork(1)
public class SingleTableOperations {
    private TableBenchmarkState state;
    BenchmarkTable bmTable;

    // @Param({"1","2","4","16"})
    @Param({"1", "4", "16"})
    private int columnCount;

    // @Param({"100", "10000", "100000", "1000000", "10000000"})
    @Param({"100", "100000", "1000000"})
    private int tableSize;

    @Param({"100", /* "90", "50", */ "10"}) // , "10", "5", "1"})
    private int sparsity;

    @Param({"int" /* , "short", "float" /*, "long", "double", "byte" */})
    private String typeName;

    private Class type;

    private Table inputTable;

    private String expression;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        type = Utils.primitiveTypeForName.get(typeName);
        Configuration.getInstance().setProperty("QueryTable.memoizeResults", "false");

        final BenchmarkTableBuilder builder = BenchmarkTools.inMemoryTableBuilder("SingleTableOperations",
                BenchmarkTools.sizeWithSparsity(tableSize, sparsity));

        builder.setSeed(0xDEADBEEF);
        for (int i = 0; i < columnCount; i++) {
            builder.addColumn(BenchmarkTools.numberCol("InputColumn" + i, type));
        }
        bmTable = builder.build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());


        inputTable = applySparsity(bmTable.getTable(), tableSize, sparsity, 0);

        expression = "(InputColumn0";
        for (int i = 1; i < columnCount; i++) {
            expression += (" + InputColumn" + i);
        }
        expression += ")";
    }

    // @TearDown(Level.Trial)
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
    }

    @TearDown(Level.Iteration)
    public void finishIteration(BenchmarkParams params) throws IOException {
        state.processResult(params);
    }

    @Benchmark
    public Table where(Blackhole bh) {
        return inputTable.where(expression + "> 0");
    }

    @Benchmark
    public Table whereSelect(Blackhole bh) {
        return inputTable.where(expression + "> 0").select();
    }

    @Benchmark
    public Table select(Blackhole bh) {
        return inputTable.select("result = " + expression);
    }

    @Benchmark
    public Table update(Blackhole bh) {
        return inputTable.update("result = " + expression);
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(SingleTableOperations.class);
    }
}
