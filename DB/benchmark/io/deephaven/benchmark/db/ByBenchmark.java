package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.TableMap;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.benchmarking.generator.EnumStringColumnGenerator;
import io.deephaven.benchmarking.generator.SequentialNumColumnGenerator;
import io.deephaven.benchmarking.impl.PersistentBenchmarkTableBuilder;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 1)
@Timeout(time = 12)
@Fork(1)
public class ByBenchmark {
    private TableBenchmarkState state;

    @Param({"Historical"})
    private String tableType;

    @Param({"operator"})
    private String mode;

    @Param({"String", "Int", "Composite"})
    private String keyType;

    @Param({"false"})
    private boolean grouped;

    @Param({"10000", "1000000"})
    private int size;

    @Param({"1000", "100000"})
    private int keyCount;

    private Table table;

    private String keyName;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        QueryTable.setMemoizeResults(false);

        final BenchmarkTableBuilder builder;

        switch (tableType) {
            case "Historical":
                builder = BenchmarkTools.persistentTableBuilder("Karl", size)
                        .setPartitioningFormula("${autobalance_single}")
                        .setPartitionCount(10);
                break;
            case "Intraday":
                builder = BenchmarkTools.persistentTableBuilder("Karl", size);
                if (grouped) {
                    throw new UnsupportedOperationException("Can not run this benchmark combination.");
                }
                break;

            default:
                throw new IllegalStateException("Table type must be Historical or Intraday");
        }

        builder.setSeed(0xDEADBEEF).addColumn(BenchmarkTools.stringCol("PartCol", 1, 5, 7, 0xFEEDBEEF));

        final EnumStringColumnGenerator stringKey = (EnumStringColumnGenerator) BenchmarkTools.stringCol("KeyString",
                keyCount, 6, 6, 0xB00FB00F, EnumStringColumnGenerator.Mode.Rotate);
        final ColumnGenerator intKey = BenchmarkTools.seqNumberCol("KeyInt", int.class, 0, 1, keyCount,
                SequentialNumColumnGenerator.Mode.RollAtLimit);

        System.out.println("Key type: " + keyType);
        switch (keyType) {
            case "String":
                builder.addColumn(stringKey);
                keyName = stringKey.getName();
                break;
            case "Int":
                builder.addColumn(intKey);
                keyName = intKey.getName();
                break;
            case "Composite":
                builder.addColumn(stringKey);
                builder.addColumn(intKey);
                keyName = stringKey.getName() + "," + intKey.getName();
                if (grouped) {
                    throw new UnsupportedOperationException("Can not run this benchmark combination.");
                }
                break;
            default:
                throw new IllegalStateException("Unknown KeyType: " + keyType);
        }

        if (grouped) {
            ((PersistentBenchmarkTableBuilder) builder).addGroupingColumns(keyName);
        }

        final BenchmarkTable bmt = builder
                .addColumn(BenchmarkTools.numberCol("Sentinel", long.class))
                .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());

        table = bmt.getTable().coalesce().dropColumns("PartCol");

        switch (mode) {
            case "chunked":
                QueryTable.USE_OLDER_CHUNKED_BY = true;
                break;
            case "operator":
                QueryTable.USE_OLDER_CHUNKED_BY = false;
                break;
            default:
                throw new IllegalArgumentException("Unknown mode " + mode);
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

    @Setup(Level.Iteration)
    public void setupIteration() {
        state.init();
    }

    @TearDown(Level.Iteration)
    public void finishIteration(BenchmarkParams params) throws IOException {
        state.processResult(params);
    }

    @Benchmark
    public Table byStatic(@NotNull final Blackhole bh) {
        final Table result =
                LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> table.by(keyName.split("[, ]+")));
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table byIncremental(@NotNull final Blackhole bh) {
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (t) -> LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> t.by(keyName.split("[, ]+"))), table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table byExternalStatic(@NotNull final Blackhole bh) {
        final TableMap result =
                LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> table.byExternal(keyName.split("[, ]+")));
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table byExternalIncremental(@NotNull final Blackhole bh) {
        final TableMap result = IncrementalBenchmark.incrementalBenchmark(
                (t) -> LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> t.byExternal(keyName.split("[, ]+"))),
                table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    public static void main(String[] args) {
        final int heapGb = 12;
        BenchUtil.run(heapGb, ByBenchmark.class);
    }
}
