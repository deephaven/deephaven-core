//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.Table;
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
public class SingleTableKeyedOperations {
    private TableBenchmarkState state;
    BenchmarkTable bmTable;

    // @QueryScopeParam({"0", "1", "2", "4"})
    @Param({"0", "1"})
    private int logColumnCount;

    // @QueryScopeParam({"100", "10000", "1000000" /*, "100000000" */})
    @Param({"1000000"})
    private int tableSize;

    @Param({"100", /* "90", "50", */ "10"}) // , "10", "5", "1"})
    private int sparsity;

    @Param({"int", /* , "short", "long", */ "byte"})
    private String typeName;

    // @QueryScopeParam({"0", "4", "8","16","31"})
    @Param({"4", "16", "31"})
    private int logSpaceSize;

    private Class type;

    private Table inputTable;

    private String[] keyColumns;


    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        int columnCount = 1 << logColumnCount;
        type = Utils.primitiveTypeForName.get(typeName);
        Configuration.getInstance().setProperty("QueryTable.memoizeResults", "false");

        final BenchmarkTableBuilder builder = BenchmarkTools.inMemoryTableBuilder("SingleTableOperations",
                BenchmarkTools.sizeWithSparsity(tableSize, sparsity));

        builder.setSeed(0xDEADBEEF).addColumn(BenchmarkTools.numberCol("Mock", int.class));
        for (int i = 0; i < columnCount; i++) {
            builder.addColumn(BenchmarkTools.numberCol("InputColumn" + i, type, 0,
                    1 << Math.max(0, logSpaceSize - logColumnCount)));
        }
        bmTable = builder.build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());


        keyColumns = new String[columnCount];
        for (int i = 0; i < keyColumns.length; i++) {
            keyColumns[i] = "InputColumn" + i;
        }
    }


    @Setup(Level.Iteration)
    public void setupIteration() {
        state.init();
        inputTable = applySparsity(bmTable.getTable().select(), tableSize, sparsity, 0);
    }


    @Benchmark
    public Table by() {
        Table result = inputTable.groupBy(keyColumns);
        return result;
    }

    @Benchmark
    public Table lastBy() {
        return inputTable.lastBy(keyColumns);
    }

    public static void main(String[] args) {
        BenchUtil.run(SingleTableKeyedOperations.class);
    }

}
