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
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 3, time = 7)
@Timeout(time = 20)
@Fork(1)
public class UngroupedColumnSourceBench extends RedirectionBenchBase {
    @Param({"10000000"})
    private int tableSize;

    @Param({"40960"})
    private int chunkCapacity;

    @Param({"true"})
    private boolean skipResultsProcessing;

    @Param({"true"})
    private boolean doSelect;

    @Param("10")
    private int steps;

    @Override
    protected QueryData getQuery() {
        final BenchmarkTableBuilder builder1;
        builder1 = BenchmarkTools.persistentTableBuilder("T1", tableSize / 10);
        builder1.setSeed(0xDEADB00F)
                .addColumn(BenchmarkTools.stringCol("PartCol1", 4, 5, 7, 0xFEEDBEEF));
        final String joinCol = "L";
        builder1.addColumn(BenchmarkTools.seqNumberCol(joinCol, long.class, 0, 1));
        builder1.addColumn(BenchmarkTools.numberCol("I1", int.class, -10_000_000, 10_000_000));
        final BenchmarkTable bmTable1 = builder1.build();

        final BenchmarkTableBuilder builder2;
        builder2 = BenchmarkTools.persistentTableBuilder("T2", tableSize);
        builder2.setSeed(0xDEADBEEF)
                .addColumn(BenchmarkTools.stringCol("PartCol2", 4, 5, 7, 0xFEEDB00F));
        builder2.addColumn(BenchmarkTools.numberCol(joinCol, long.class, 0, tableSize));
        final BenchmarkTable bmTable2 = builder2.build();
        final Table t1 = bmTable1.getTable().coalesce();
        final Table t2 = bmTable2.getTable().coalesce();
        final long sizePerStep = Math.max(t1.size() / steps, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
                new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table live;
        if (doSelect) {
            live = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(
                    () -> t1.where(incrementalReleaseFilter).select(joinCol, "PartCol1", "I1").sort("I1").join(
                            t2, joinCol, "PartCol2"));
        } else {
            live = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(
                    () -> t1.where(incrementalReleaseFilter).sort("I1").join(
                            t2, joinCol, "PartCol2"));
        }
        return new QueryData(
                live,
                incrementalReleaseFilter,
                steps,
                new String[] {joinCol},
                WritableLongChunk.makeWritableChunk(chunkCapacity));
    }

    public static void main(final String[] args) {
        BenchUtil.run(UngroupedColumnSourceBench.class);
    }
}
