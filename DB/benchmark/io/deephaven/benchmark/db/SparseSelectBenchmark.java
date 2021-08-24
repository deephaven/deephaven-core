package io.deephaven.benchmark.db;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.SparseSelect;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 1)
@Timeout(time = 3)
@Fork(1)
public class SparseSelectBenchmark {
    private TableBenchmarkState state;
    private BenchmarkTable bmTable;

    @Param({"Intraday"})
    private String tableType;

    @Param({"250000", "2500000", "25000000"})
    private int tableSize;

    @Param({"100", "50", "25"})
    private int sparsity;
    private Table inputTable;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        final int actualSize = BenchmarkTools.sizeWithSparsity(tableSize, sparsity);

        System.out.println("Actual Size: " + actualSize);

        final BenchmarkTableBuilder builder =
            BenchmarkTools.persistentTableBuilder("Carlos", actualSize);
        bmTable = builder
            .setSeed(0xDEADBEEF)
            .addColumn(BenchmarkTools.stringCol("PartCol", 4, 5, 7, 0xFEEDBEEF))
            .addColumn(BenchmarkTools.stringCol("Stringy", 1, 10))
            .addColumn(BenchmarkTools.numberCol("I1", int.class))
            .addColumn(BenchmarkTools.numberCol("D1", double.class, -10e6, 10e6))
            .addColumn(BenchmarkTools.numberCol("L1", long.class))
            .addColumn(BenchmarkTools.numberCol("B1", byte.class))
            .addColumn(BenchmarkTools.numberCol("S1", short.class))
            .addColumn(BenchmarkTools.numberCol("F1", float.class))
            .addColumn(BenchmarkTools.charCol("C1", 'A', 'Z'))
            .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()),
            params.getWarmup().getCount());

        inputTable = applySparsity(bmTable.getTable(), tableSize, sparsity, 0).coalesce();
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
    }

    @TearDown(Level.Iteration)
    public void finishIteration(BenchmarkParams params) throws IOException {
        state.processResult(params);
        state.reset();
    }

    @Benchmark
    public Table incrementalSparseSelect() {
        final Table result =
            LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> IncrementalBenchmark
                .incrementalBenchmark(SparseSelect::sparseSelect, inputTable, 10));
        Assert.eq(result.size(), "result.size()", inputTable.size(), "inputTable.size()");
        return state.setResult(result);
    }

    @Benchmark
    public Table sparseSelect() {
        return state.setResult(LiveTableMonitor.DEFAULT.exclusiveLock()
            .computeLocked(() -> SparseSelect.sparseSelect(inputTable)));
    }

    public static void main(final String[] args) {
        final int heapGb = 16;
        BenchUtil.run(heapGb, SparseSelectBenchmark.class);
    }
}
