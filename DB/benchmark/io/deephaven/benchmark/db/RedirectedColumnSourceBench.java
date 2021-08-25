package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 3, time = 7)
@Timeout(time = 20)
@Fork(1)
public class RedirectedColumnSourceBench extends RedirectionBenchBase {
    // @Param({"D", "F", "I", "S"})
    @Param({"D"})
    private String fillColsType;

    @Param({"1", "2", "6", "20"})
    private int nFillCols;

    @Param({"10000000"})
    private int tableSize;

    @Param({"4096"})
    private int chunkCapacity;

    @Param({"true"})
    private boolean skipResultsProcessing;

    @Param({"true"})
    private boolean doSelect;

    @Param("10")
    private int steps;

    @Override
    protected QueryData getQuery() {
        if (nFillCols < 1) {
            throw new InternalError("Invalid nFillCols=" + nFillCols);
        }
        final BenchmarkTableBuilder builder;
        builder = BenchmarkTools.persistentTableBuilder("Juancho", tableSize);
        builder.setSeed(0xDEADBEEF)
            .addColumn(BenchmarkTools.stringCol("PartCol", 4, 5, 7, 0xFEEDBEEF));
        final WritableChunk<Attributes.Values> chunk;
        final Consumer<String> builderAddColumn;
        final int nSelectCols;
        switch (fillColsType) {
            case "D":
                builderAddColumn = (final String col) -> builder
                    .addColumn(BenchmarkTools.numberCol(col, double.class, -10e6, 10e6));
                chunk = WritableDoubleChunk.makeWritableChunk(chunkCapacity);
                nSelectCols = nFillCols + 1;
                break;
            case "F":
                builderAddColumn = (final String col) -> builder
                    .addColumn(BenchmarkTools.numberCol(col, float.class, -10e6f, 10e6f));
                chunk = WritableFloatChunk.makeWritableChunk(chunkCapacity);
                nSelectCols = nFillCols + 1;
                break;
            case "L":
                builderAddColumn = (final String col) -> builder
                    .addColumn(BenchmarkTools.numberCol(col, long.class, -10_000_000, 10_000_000));
                chunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
                nSelectCols = nFillCols + 1;
                break;
            case "I":
                builderAddColumn = (final String col) -> builder
                    .addColumn(BenchmarkTools.numberCol(col, int.class, -10_000_000, 10_000_000));
                chunk = WritableIntChunk.makeWritableChunk(chunkCapacity);
                nSelectCols = nFillCols;
                break;
            case "S":
                builderAddColumn = (final String col) -> builder
                    .addColumn(BenchmarkTools.stringCol(col, 4096, 4, 8, 0xDEADBEEF));
                chunk = WritableObjectChunk.makeWritableChunk(chunkCapacity);
                nSelectCols = nFillCols + 1;
                break;
            default:
                throw new InternalError("Unhandled column type");
        }
        final String[] selectCols = new String[nSelectCols];
        final String[] fillCols = new String[nFillCols];
        for (int i = 0; i < nFillCols; ++i) {
            final String col = fillColsType + i;
            builderAddColumn.accept(col);
            fillCols[i] = col;
            selectCols[i] = col;
        }
        final String sortCol = "I0";
        if (fillColsType != "I") {
            builderAddColumn.accept(sortCol);
            selectCols[nSelectCols - 1] = sortCol;
        }

        final BenchmarkTable bmTable = builder.build();
        final Table t = bmTable.getTable().coalesce();
        final long sizePerStep = Math.max(t.size() / steps, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
            new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table live;
        if (doSelect) {
            live = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(
                () -> t.where(incrementalReleaseFilter).select(selectCols).sort(sortCol));
        } else {
            live = t.where(incrementalReleaseFilter).sort(sortCol);
        }
        return new QueryData(
            live,
            incrementalReleaseFilter,
            steps,
            fillCols,
            chunk);
    }

    public static void main(String[] args) {
        BenchUtil.run(RedirectedColumnSourceBench.class);
    }
}
