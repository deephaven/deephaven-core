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
@Timeout(time = 2)
@Fork(1)
public class MultiTableKeyedOperations {
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

    @Param({"int", /* , "short", "long", */ "byte"})
    private String typeName;

    // @Param({"0", "4", "8","16","31"})
    @Param({"4", "16", "31"})
    private int logSpaceSize;

    private Class type;

    private Table distinctTable;
    private Table fullTable;

    private String[] keyColumns;
    private String columnsToMatch;


    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        int columnCount = 1 << logColumnCount;
        type = Utils.primitiveTypeForName.get(typeName);
        Configuration.getInstance().setProperty("QueryTable.memoizeResults", "false");

        final BenchmarkTableBuilder builder = BenchmarkTools.inMemoryTableBuilder(
            "SingleTableOperations", BenchmarkTools.sizeWithSparsity(tableSize, sparsity));

        builder.setSeed(0xDEADBEEF).addColumn(BenchmarkTools.numberCol("Mock", int.class));
        for (int i = 0; i < columnCount; i++) {
            builder.addColumn(BenchmarkTools.numberCol("InputColumn" + i, type, 0,
                1 << Math.max(0, logSpaceSize - logColumnCount)));
        }
        bmTable = builder.build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()),
            params.getWarmup().getCount());


        keyColumns = new String[columnCount];
        for (int i = 0; i < keyColumns.length; i++) {
            keyColumns[i] = "InputColumn" + i;
        }
        columnsToMatch = String.join(",", keyColumns);
        fullTable = applySparsity(bmTable.getTable().select(), tableSize, sparsity, 0);
        distinctTable = applySparsity(bmTable.getTable().select(), tableSize, sparsity, 0)
            .renameColumns("DMock = Mock").lastBy(keyColumns).select();
    }


    @Setup(Level.Iteration)
    public void setupIteration() {
        state.init();
    }


    @Benchmark
    public Table naturalJoin() {

        Table result = fullTable.naturalJoin(distinctTable, columnsToMatch);
        return result;
    }

    @Benchmark
    public Table leftJoin() {

        return distinctTable.leftJoin(fullTable, columnsToMatch);
    }

    public static void main(String[] args) {
        BenchUtil.run(MultiTableKeyedOperations.class);
    }
}
