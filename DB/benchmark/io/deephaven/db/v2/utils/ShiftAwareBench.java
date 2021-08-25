package io.deephaven.db.v2.utils;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import io.deephaven.benchmarking.BenchUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 10, time = 3)
@Fork(1)
public class ShiftAwareBench {
    private TableBenchmarkState state;
    private BenchmarkTable bmTable;
    private Table inputTable;

    @Param({"100", "100000", "10000000"})
    private int tableSize;

    @Param({"100", "50", "20"})
    private int sparsity;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        Configuration.getInstance().setProperty("QueryTable.memoizeResults", "false");

        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        final BenchmarkTableBuilder builder = BenchmarkTools.inMemoryTableBuilder("ShiftAwareBench",
                BenchmarkTools.sizeWithSparsity(tableSize, sparsity));

        builder.setSeed(0xDEADBEEF);
        builder.addColumn(BenchmarkTools.numberCol("intCol", Integer.class, 0, 1 << 20));
        bmTable = builder.build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());

        inputTable = applySparsity(bmTable.getTable(), tableSize, sparsity, 0);
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

    private <R> R incrementalBenchmark(Function<Table, R> function) {
        final long sizePerStep = Math.max(inputTable.size() / 10, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
                new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table filtered = inputTable.where(incrementalReleaseFilter);

        final R result = function.apply(filtered);

        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        while (filtered.size() < inputTable.size()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);
        }

        return result;
    }

    @Benchmark
    public Table b00_flattenWhere() {
        final Table result = incrementalBenchmark((Table t) -> t.flatten().where("intCol % 3 == 0"));
        return state.setResult(result);
    }

    @Benchmark
    public Table b01_mergeWhere() {
        final Table result = incrementalBenchmark((Table t) -> TableTools.merge(t, t).where("intCol % 3 == 0"));
        return state.setResult(result);
    }

    @Benchmark
    public Table b02_mergeFlattenWhere() {
        final Table result =
                incrementalBenchmark((Table t) -> TableTools.merge(t, t).flatten().where("intCol % 3 == 0"));
        return state.setResult(result);
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(ShiftAwareBench.class);
    }
}
