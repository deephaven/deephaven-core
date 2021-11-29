package io.deephaven.benchmark.engine;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@SuppressWarnings("unused")
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 10, time = 5)
@Timeout(time = 10)
@Fork(1)
public class RegionedColumnSourceBenchmark {

    private TableBenchmarkState state;

    @Param({"I1", "L1", "Symbol", "Timestamp"})
    private String fillColumn;

    @Param({"128", "1024", "8192", "65536"})
    private int chunkCapacity;

    @Param({"Fill", "Get" /* , "Default", "Legacy" */})
    private String mode;

    @Param({"false", "true"})
    private String useNativeByteOrder;

    @Param({"false", "true"})
    private String useDirectBuffers;

    @Param({"Historical"})
    private String tableType;

    @Param({"10000000"})
    private int tableSize;

    @Param({"100", "50"})
    private int sparsity;

    private Table inputTable;
    private WritableChunk<Values> destination;
    private Copier copier;

    private enum Copier {
        Int() {
            @Override
            final void copy(@NotNull final ColumnSource columnSource, @NotNull final WritableChunk<Values> destination,
                    final long key) {
                destination.asWritableIntChunk().add(columnSource.getInt(key));
            }
        },
        Long() {
            @Override
            final void copy(@NotNull final ColumnSource columnSource, @NotNull final WritableChunk<Values> destination,
                    final long key) {
                destination.asWritableLongChunk().add(columnSource.getLong(key));
            }
        },
        Object() {
            @Override
            final void copy(@NotNull final ColumnSource columnSource, @NotNull final WritableChunk<Values> destination,
                    final long key) {
                destination.asWritableObjectChunk().add(columnSource.get(key));
            }
        };

        abstract void copy(@NotNull ColumnSource columnSource, @NotNull WritableChunk<Values> destination, long key);
    }

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        Configuration.getInstance().setProperty(UpdateGraphProcessor.ALLOW_UNIT_TEST_MODE_PROP, "true");

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();

        final BenchmarkTableBuilder builder;
        final int actualSize = BenchmarkTools.sizeWithSparsity(tableSize, sparsity);

        switch (tableType) {
            case "Historical":
                builder = BenchmarkTools.persistentTableBuilder("RegionedTable", actualSize)
                        .setPartitioningFormula("${autobalance_single}")
                        .setPartitionCount(10);
                break;
            case "Intraday":
                builder = BenchmarkTools.persistentTableBuilder("RegionedTable", actualSize);
                break;

            default:
                throw new IllegalStateException("Table type must be Historical, or Intraday");
        }

        builder.setSeed(0xDEADBEEF)
                .addColumn(BenchmarkTools.stringCol("PartitioningColumn", 4, 5, 7, 0xFEEDBEEF));

        switch (fillColumn) {
            case "I1":
                builder.addColumn(BenchmarkTools.numberCol("I1", int.class, 0, 1000));
                destination = ChunkType.Int.makeWritableChunk(chunkCapacity);
                copier = Copier.Int;
                break;
            case "L1":
                builder.addColumn(BenchmarkTools.numberCol("L1", long.class, 0, 1000));
                destination = ChunkType.Long.makeWritableChunk(chunkCapacity);
                copier = Copier.Long;
                break;
            case "Symbol":
                builder.addColumn(BenchmarkTools.stringCol("Symbol", 1000, 1, 10, 0));
                destination = ChunkType.Object.makeWritableChunk(chunkCapacity);
                copier = Copier.Object;
                break;
            case "Timestamp":
                builder.addColumn(BenchmarkTools.dateCol("Timestamp"));
                destination = ChunkType.Object.makeWritableChunk(chunkCapacity);
                copier = Copier.Object;
                break;
        }

        final BenchmarkTable bmTable = builder.build();
        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());
        inputTable = applySparsity(bmTable.getTable(), tableSize, sparsity, 555).coalesce();
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
    }

    @Benchmark
    public void readEntireTable(@NotNull final Blackhole bh) {
        final AbstractColumnSource inputSource = (AbstractColumnSource) inputTable.getColumnSource(fillColumn);
        switch (mode) {
            case "Fill":
                try (final ColumnSource.FillContext fillContext = inputSource.makeFillContext(chunkCapacity);
                        final RowSequence.Iterator rsi = inputTable.getRowSet().getRowSequenceIterator()) {
                    while (rsi.hasMore()) {
                        final RowSequence rs = rsi.getNextRowSequenceWithLength(chunkCapacity);
                        inputSource.fillChunk(fillContext, destination, rs);
                        bh.consume(destination);
                    }
                }
                break;
            case "Get":
                try (final ColumnSource.GetContext getContext = inputSource.makeGetContext(chunkCapacity);
                        final RowSequence.Iterator rsi = inputTable.getRowSet().getRowSequenceIterator()) {
                    while (rsi.hasMore()) {
                        final RowSequence rs = rsi.getNextRowSequenceWithLength(chunkCapacity);
                        bh.consume(inputSource.getChunk(getContext, rs));
                    }
                }
                break;
            case "Default":
                try (final ColumnSource.FillContext fillContext = inputSource.makeFillContext(chunkCapacity);
                        final RowSequence.Iterator rsi = inputTable.getRowSet().getRowSequenceIterator()) {
                    while (rsi.hasMore()) {
                        final RowSequence rs = rsi.getNextRowSequenceWithLength(chunkCapacity);
                        inputSource.defaultFillChunk(fillContext, destination, rs);
                        bh.consume(destination);
                    }
                }
                break;
            case "Legacy":
                for (final RowSet.Iterator ii = inputTable.getRowSet().iterator(); ii.hasNext();) {
                    destination.setSize(0);
                    ii.forEachLong(k -> {
                        copier.copy(inputSource, destination, k);
                        return destination.size() < chunkCapacity;
                    });
                    bh.consume(destination);
                }
                break;
            default:
                throw new IllegalStateException("Mode must be Fill, Get, Default, or Legacy");
        }
        state.setResult(TableTools.emptyTable(0));
    }

    public static void main(@NotNull final String... args) {
        BenchUtil.run(RegionedColumnSourceBenchmark.class);
    }
}
