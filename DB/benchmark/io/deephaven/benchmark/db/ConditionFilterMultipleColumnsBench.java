package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.select.ConditionFilter;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 3, time = 7)
@Timeout(time = 20)
@Fork(1)
public class ConditionFilterMultipleColumnsBench {
    protected TableBenchmarkState state;
    private boolean skipResultsProcessing = true;
    @Param({"400000"})
    private int tableSize;
    @Param({"10"})
    private int steps;
    @Param({"1", "2", "10"})
    private int nFilterCols;
    @Param({"0"})
    private int nAdditionalCols;
    @Param({"5"})
    private int pctFilteredOut;
    @Param({"false"})
    private boolean doSelect;

    private Table inputTable;
    private String[] tCols;
    private String filterExpression;
    private String sortCol;

    @Setup(Level.Trial)
    public void setupEnv(final BenchmarkParams params) {
        if (nFilterCols < 1 || nAdditionalCols < 0) {
            throw new IllegalArgumentException();
        }
        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());
        final BenchmarkTableBuilder builder;
        final String tPartCol = "TPartCol";
        builder = BenchmarkTools.persistentTableBuilder("T", tableSize);
        builder.setSeed(0xDEADB00F)
                .addColumn(BenchmarkTools.stringCol(tPartCol, 4, 5, 7, 0xFEEDBEEF));
        tCols = new String[2 + nFilterCols + nAdditionalCols];
        int nT1Cols = 0;
        tCols[nT1Cols++] = tPartCol;
        sortCol = "SortCol";
        builder.addColumn(BenchmarkTools.numberCol(sortCol, long.class, -10_000_000, 10_000_000));
        tCols[nT1Cols++] = sortCol;
        final String filterColPrefix = "L";
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nFilterCols; ++i) {
            final String col = filterColPrefix + i;
            if (i != 0) {
                sb.append(" && ");
            }
            sb.append(col).append(" > " + pctFilteredOut);
            builder.addColumn(BenchmarkTools.numberCol(col, long.class, 0, 99));
            tCols[nT1Cols++] = col;
        }
        filterExpression = sb.toString();
        for (int i = 0; i < nAdditionalCols; ++i) {
            final String col = "I" + i;
            builder.addColumn(BenchmarkTools.numberCol(col, int.class, -10_000_000, 10_000_000));
            tCols[nT1Cols++] = col;
        }
        final BenchmarkTable bmTable = builder.build();
        final Table t = bmTable.getTable();
        if (doSelect) {
            inputTable = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(
                    () -> t.select(tCols).sort(sortCol).coalesce());
        } else {
            inputTable = t.sort(sortCol).coalesce();

        }
    }

    @TearDown(Level.Trial)
    public void finishTrial() {
        try {
            state.logOutput();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Supplier<Table> bench;

    @Setup(Level.Invocation)
    public void setupInvocation() {
        final long sizePerStep = Math.max(inputTable.size() / steps, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
                new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table inputReleased = inputTable.where(incrementalReleaseFilter);

        final SelectFilter filter = ConditionFilter.createConditionFilter(filterExpression);
        final Table result = inputReleased.where(filter);
        // Compute the first pass of live iterations outside of the bench measurement,
        // to avoid including the time to setup the filter itself.
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);
        final long fullyReleasedSize = inputTable.size();
        bench = () -> {
            while (inputReleased.size() < fullyReleasedSize) {
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);
            }
            return result;
        };
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        bench = null;
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        state.init();
    }

    @TearDown(Level.Iteration)
    public void finishIteration(BenchmarkParams params) throws IOException {
        if (skipResultsProcessing) {
            return;
        }
        state.processResult(params);
    }

    @Benchmark
    public Table conditionFilterBench(final Blackhole bh) {
        final Table result = bench.get();
        return state.setResult(result);
    }

    public static void main(String[] args) {
        final int heapGb = 12;
        BenchUtil.run(heapGb, ConditionFilterMultipleColumnsBench.class);
    }
}
