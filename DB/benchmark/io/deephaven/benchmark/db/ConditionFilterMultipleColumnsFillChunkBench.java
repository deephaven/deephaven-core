package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.select.ConditionFilter;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 28)
@Measurement(iterations = 3, time = 21)
@Timeout(time = 40)
@Fork(1)
public class ConditionFilterMultipleColumnsFillChunkBench extends RedirectionBenchBase {
    private boolean skipResultsProcessing = true;
    @Param({"4000000"})
    private int tableSize;
    @Param({"true"})
    private boolean doSort;
    @Param({"10"})
    private int steps;
    @Param({"10"})
    private int numberOfFilterColumns;
    @Param({"0"})
    private int numberOfAdditionalColumns;
    @Param({"4096"})
    private int chunkCapacity;

    @Override
    protected QueryData getQuery() {
        if (numberOfFilterColumns < 1 || numberOfAdditionalColumns < 0) {
            throw new IllegalArgumentException();
        }

        final BenchmarkTableBuilder builder;
        final String tPartCol = "TPartCol";
        builder = BenchmarkTools.persistentTableBuilder("T", tableSize);
        builder.setSeed(0xDEADB00F)
            .addColumn(BenchmarkTools.stringCol(tPartCol, 4, 5, 7, 0xFEEDBEEF));
        final String[] tCols = new String[2 + numberOfFilterColumns + numberOfAdditionalColumns];
        int nT1Cols = 0;
        tCols[nT1Cols++] = tPartCol;
        String sortCol = "SortCol";
        builder.addColumn(BenchmarkTools.numberCol(sortCol, long.class, -10_000_000, 10_000_000));
        final String filterColPrefix = "L";
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfFilterColumns; ++i) {
            final String col = filterColPrefix + i;
            if (i != 0) {
                sb.append(" && ");
            }
            sb.append(col).append(" <= 95");
            builder.addColumn(BenchmarkTools.numberCol(col, long.class, 0, 99));
            tCols[nT1Cols++] = col;
        }
        final String filterExpression = sb.toString();
        for (int i = 0; i < numberOfAdditionalColumns; ++i) {
            final String col = "I" + i;
            builder.addColumn(BenchmarkTools.numberCol(col, int.class, -10_000_000, 10_000_000));
            tCols[nT1Cols++] = col;
        }
        final BenchmarkTable bmTable = builder.build();
        final Table inputTable = bmTable.getTable().coalesce();
        final long sizePerStep = Math.max(inputTable.size() / steps, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
            new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table inputReleased = inputTable.where(incrementalReleaseFilter);

        final SelectFilter filter = ConditionFilter.createConditionFilter(filterExpression);
        final Table live = inputReleased.sort(sortCol).where(filter);
        return new QueryData(live, incrementalReleaseFilter, steps, new String[] {sortCol},
            WritableLongChunk.makeWritableChunk(chunkCapacity));
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

    public static void main(String[] args) {
        BenchUtil.run(ConditionFilterMultipleColumnsFillChunkBench.class);
    }
}
