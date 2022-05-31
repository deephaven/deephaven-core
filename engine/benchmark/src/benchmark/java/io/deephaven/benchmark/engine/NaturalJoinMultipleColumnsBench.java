package io.deephaven.benchmark.engine;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 15)
@Measurement(iterations = 3, time = 15)
@Timeout(time = 30)
@Fork(1)
public class NaturalJoinMultipleColumnsBench {
    protected TableBenchmarkState state;
    @Param({"true"})
    private boolean skipResultsProcessing = true;
    @Param({"409600"})
    private int tableSize;
    @Param({"true"})
    private boolean doSelect;
    @Param({"5"})
    private int steps;
    @Param({"1", "2", "4"})
    private int numberOfJoinColumns;
    @Param({"1"})
    private int t1NumberOfAdditionalColumns;
    @Param({"0"})
    private int t2NumberOfAdditionalColumns;

    private Table inputTable, t2;
    private String[] t1Cols;
    private String sortCol;
    private String joinColsStr;
    private String joinColumnsToAddStr;

    @Setup(Level.Trial)
    public void setupEnv(final BenchmarkParams params) {
        if (numberOfJoinColumns < 1 || t1NumberOfAdditionalColumns < 1) {
            throw new InternalError(
                    "Both numberOfJoinColumns(=" + numberOfJoinColumns + ") and t1NumberOfAdditionalColumns(="
                            + t1NumberOfAdditionalColumns + ") have to be >= 1.");
        }
        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        final BenchmarkTableBuilder builder1;
        final String t1PartCol = "T1PartCol";
        builder1 = BenchmarkTools.persistentTableBuilder("T1", tableSize);
        builder1.setSeed(0xDEADB00F)
                .addColumn(BenchmarkTools.stringCol(t1PartCol, 4, 5, 7, 0xFEEDBEEF));
        t1Cols = new String[numberOfJoinColumns + t1NumberOfAdditionalColumns + 1];
        int nT1Cols = 0;
        t1Cols[nT1Cols++] = t1PartCol;
        final String[] joinCols = new String[numberOfJoinColumns];
        for (int i = 0; i < numberOfJoinColumns; ++i) {
            final String col = "J" + i;
            builder1.addColumn(BenchmarkTools.seqNumberCol(col, long.class, -tableSize, 0));
            t1Cols[nT1Cols++] = col;
            joinCols[i] = col;
        }

        for (int i = 0; i < t1NumberOfAdditionalColumns; ++i) {
            final String col = "T1I" + i;
            builder1.addColumn(BenchmarkTools.numberCol(col, int.class, -10_000_000, 10_000_000));
            t1Cols[nT1Cols++] = col;
            if (i == 0) {
                sortCol = col;
            }
        }
        final BenchmarkTable bmTable1 = builder1.build();

        final BenchmarkTableBuilder builder2;
        final String t2PartCol = "T2PartCol";
        builder2 = BenchmarkTools.persistentTableBuilder("T2", tableSize);
        builder2.setSeed(0xDEADBEEF)
                .addColumn(BenchmarkTools.stringCol(t2PartCol, 4, 5, 7, 0xFEEDB00F));
        final String[] t2Cols = new String[numberOfJoinColumns + t2NumberOfAdditionalColumns + 1];
        int nT2Cols = 0;
        t2Cols[nT2Cols++] = t2PartCol;
        for (int i = 0; i < numberOfJoinColumns; ++i) {
            final String col = "J" + i;
            builder2.addColumn(BenchmarkTools.seqNumberCol(col, long.class, 0, tableSize));
            t2Cols[nT2Cols++] = col;
        }
        final String[] joinColumnsToAdd = new String[t2NumberOfAdditionalColumns];
        for (int i = 0; i < t2NumberOfAdditionalColumns; ++i) {
            final String col = "T2I" + i;
            builder2.addColumn(BenchmarkTools.numberCol(col, int.class, -10_000_000, 10_000_000));
            t2Cols[nT2Cols++] = col;
            joinColumnsToAdd[i] = col;
        }
        final BenchmarkTable bmTable2 = builder2.build();
        final Table t1 = bmTable1.getTable().coalesce();
        inputTable = t1;
        t2 = bmTable2.getTable().coalesce();
        joinColsStr = String.join(",", joinCols);
        joinColumnsToAddStr = String.join(",", joinColumnsToAdd);
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
        if (skipResultsProcessing) {
            return;
        }
        state.processResult(params);
    }

    @Benchmark
    public Table naturalJoinBench(final Blackhole bh) {
        final Table result;
        if (doSelect) {
            result = UpdateGraphProcessor.DEFAULT.exclusiveLock()
                    .computeLocked(() -> IncrementalBenchmark
                            .incrementalBenchmark((Table t) -> t.select(t1Cols).sort(sortCol).naturalJoin(
                                    t2, joinColsStr, joinColumnsToAddStr), inputTable, steps));
        } else {
            result = IncrementalBenchmark.incrementalBenchmark((Table t) -> t.sort(sortCol).naturalJoin(
                    t2, joinColsStr, joinColumnsToAddStr), inputTable, steps);
        }
        return state.setResult(result);
    }

    public static void main(String[] args) {
        BenchUtil.run(NaturalJoinMultipleColumnsBench.class);
    }
}
