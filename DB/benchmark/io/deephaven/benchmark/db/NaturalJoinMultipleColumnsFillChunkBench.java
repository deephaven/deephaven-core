package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import org.openjdk.jmh.annotations.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 15)
@Measurement(iterations = 3, time = 15)
@Timeout(time = 30)
@Fork(1)
public class NaturalJoinMultipleColumnsFillChunkBench extends RedirectionBenchBase {
    @Param({"true"})
    private boolean skipResultsProcessing = true;
    @Param({"2000000"})
    private int tableSize;
    @Param({"true"})
    private boolean doSelect;
    @Param({"100"})
    private int steps;
    @Param({"1", "2", "5", "15"})
    private int numberOfJoinColumns;
    @Param({"0"})
    private int t1NumberOfAdditionalColumns;
    @Param({"0"})
    private int t2NumberOfAdditionalColumns;
    @Param({"4096"})
    private int chunkCapacity;

    private String[] t1Cols;
    private String sortCol;

    @Override
    protected QueryData getQuery() {
        if (numberOfJoinColumns < 1) {
            throw new IllegalArgumentException("numberOfJoinColumns must be >= 1");
        }
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
            if (i == 0) {
                sortCol = col;
            }
            t1Cols[nT1Cols++] = col;
            joinCols[i] = col;
        }

        for (int i = 0; i < t1NumberOfAdditionalColumns; ++i) {
            final String col = "T1I" + i;
            builder1.addColumn(BenchmarkTools.numberCol(col, int.class, -10_000_000, 10_000_000));
            t1Cols[nT1Cols++] = col;
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
        final Table t2 = bmTable2.getTable().coalesce();
        final long sizePerStep = Math.max(t1.size() / steps, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
            new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table t1Released = t1.where(incrementalReleaseFilter);
        final Table live;
        final String joinColsStr = String.join(",", joinCols);
        final String joinColumnsToAddStr = String.join(",", joinColumnsToAdd);
        if (doSelect) {
            live = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> t1Released
                .select(t1Cols).sort(sortCol).naturalJoin(t2, joinColsStr, joinColumnsToAddStr));
        } else {
            live = t1Released.sort(sortCol).naturalJoin(t2, joinColsStr, joinColumnsToAddStr);
        }
        return new QueryData(
            live, incrementalReleaseFilter, steps, joinCols,
            WritableLongChunk.makeWritableChunk(chunkCapacity));
    }

    public static void main(String[] args) {
        BenchUtil.run(NaturalJoinMultipleColumnsFillChunkBench.class);
    }
}
