/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmark.engine;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.benchmarking.generator.EnumStringGenerator;
import io.deephaven.benchmarking.generator.SequentialNumberGenerator;
import io.deephaven.benchmarking.impl.PersistentBenchmarkTableBuilder;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.deephaven.api.agg.Aggregation.AggMax;
import static io.deephaven.api.agg.Aggregation.AggMin;

@SuppressWarnings("unused")
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 1)
@Timeout(time = 15)
@Fork(1)
public class SumByBenchmark {
    private TableBenchmarkState state;

    @Param({"Historical"})
    private String tableType;

    @Param({"String", "Int", "Composite", "None"})
    private String keyType;

    @Param({"false", "true"})
    private boolean grouped;

    @Param({"10000000"})
    private int size;

    @Param({"1000", "1000000"}) // 0 can only be used with "Hone" keyType.
    private int keyCount;

    @Param({"1", "8"})
    private int valueCount;

    private Table table;

    private String keyName;
    private String[] keyColumnNames;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        TestExecutionContext.createForUnitTests().open();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();
        QueryTable.setMemoizeResults(false);

        final BenchmarkTableBuilder builder;

        if (keyCount == 0) {
            if (!"None".equals(keyType)) {
                throw new UnsupportedOperationException("Zero Key can only be run with keyType == None");
            }
        } else {
            if ("None".equals(keyType)) {
                throw new UnsupportedOperationException("keyType == None can only be run with keyCount==0");
            }
        }

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

        final ColumnGenerator<String> stringKey = BenchmarkTools.stringCol(
                "KeyString", keyCount, 6, 6, 0xB00FB00FL, EnumStringGenerator.Mode.Rotate);
        final ColumnGenerator<Integer> intKey = BenchmarkTools.seqNumberCol(
                "KeyInt", int.class, 0, 1, keyCount, SequentialNumberGenerator.Mode.RollAtLimit);

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
            case "None":
                keyName = "<bad key name for None>";
                if (grouped) {
                    throw new UnsupportedOperationException("Can not run this benchmark combination.");
                }
                break;
            default:
                throw new IllegalStateException("Unknown KeyType: " + keyType);
        }
        keyColumnNames = keyCount > 0 ? keyName.split(",") : CollectionUtil.ZERO_LENGTH_STRING_ARRAY;

        switch (valueCount) {
            case 8:
                builder.addColumn(BenchmarkTools.numberCol("ValueToSum8", float.class));
            case 7:
                builder.addColumn(BenchmarkTools.numberCol("ValueToSum7", long.class));
            case 6:
                builder.addColumn(BenchmarkTools.numberCol("ValueToSum6", double.class));
            case 5:
                builder.addColumn(BenchmarkTools.numberCol("ValueToSum5", int.class));
            case 4:
                builder.addColumn(BenchmarkTools.numberCol("ValueToSum4", float.class));
            case 3:
                builder.addColumn(BenchmarkTools.numberCol("ValueToSum3", long.class));
            case 2:
                builder.addColumn(BenchmarkTools.numberCol("ValueToSum2", double.class));
            case 1:
                builder.addColumn(BenchmarkTools.numberCol("ValueToSum1", int.class));
                break;
            default:
                throw new IllegalArgumentException("Can not initialize with " + valueCount + " values.");
        }

        if (grouped) {
            ((PersistentBenchmarkTableBuilder) builder).addGroupingColumns(keyName);
        }

        final BenchmarkTable bmt = builder
                .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());

        table = bmt.getTable().coalesce().dropColumns("PartCol");
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
    public Table sumByStatic(@NotNull final Blackhole bh) {
        final Table result = ExecutionContext.getContext().getUpdateGraph().sharedLock()
                .computeLocked(() -> table.sumBy(keyColumnNames));
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table sumByIncremental(@NotNull final Blackhole bh) {
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock()
                            .computeLocked(() -> t.sumBy(keyColumnNames));
                }, table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table sumByRolling(@NotNull final Blackhole bh) {
        final Table result = IncrementalBenchmark.rollingBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock()
                            .computeLocked(() -> t.sumBy(keyColumnNames));
                }, table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table minByStatic(@NotNull final Blackhole bh) {
        final Table result = ExecutionContext.getContext().getUpdateGraph().sharedLock()
                .computeLocked(() -> table.minBy(keyColumnNames));
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table minByIncremental(@NotNull final Blackhole bh) {
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock()
                            .computeLocked(() -> t.minBy(keyColumnNames));
                }, table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table minByRolling(@NotNull final Blackhole bh) {
        final Table result = IncrementalBenchmark.rollingBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock()
                            .computeLocked(() -> t.minBy(keyColumnNames));
                }, table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table minMaxByStatic(@NotNull final Blackhole bh) {
        final Aggregation minCols = AggMin(IntStream.range(1, valueCount + 1)
                .mapToObj(ii -> "Min" + ii + "=ValueToSum" + ii).toArray(String[]::new));
        final Aggregation maxCols = AggMax(IntStream.range(1, valueCount + 1)
                .mapToObj(ii -> "Max" + ii + "=ValueToSum" + ii).toArray(String[]::new));

        final Table result = ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                () -> table.aggBy(List.of(minCols, maxCols), keyColumnNames));
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table minMaxByIncremental(@NotNull final Blackhole bh) {
        final Aggregation minCols = AggMin(IntStream.range(1, valueCount + 1)
                .mapToObj(ii -> "Min" + ii + "=ValueToSum" + ii).toArray(String[]::new));
        final Aggregation maxCols = AggMax(IntStream.range(1, valueCount + 1)
                .mapToObj(ii -> "Max" + ii + "=ValueToSum" + ii).toArray(String[]::new));

        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                            () -> t.aggBy(List.of(minCols, maxCols), keyColumnNames));
                },
                table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table minMaxByRolling(@NotNull final Blackhole bh) {
        final Aggregation minCols = AggMin(IntStream.range(1, valueCount + 1)
                .mapToObj(ii -> "Min" + ii + "=ValueToSum" + ii).toArray(String[]::new));
        final Aggregation maxCols = AggMax(IntStream.range(1, valueCount + 1)
                .mapToObj(ii -> "Max" + ii + "=ValueToSum" + ii).toArray(String[]::new));

        final Table result = IncrementalBenchmark.rollingBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                            () -> t.aggBy(List.of(minCols, maxCols), keyColumnNames));
                },
                table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table varByStatic(@NotNull final Blackhole bh) {
        final Table result = ExecutionContext.getContext().getUpdateGraph().sharedLock()
                .computeLocked(() -> table.varBy(keyColumnNames));
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table varByIncremental(@NotNull final Blackhole bh) {
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock()
                            .computeLocked(() -> t.varBy(keyColumnNames));
                }, table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table avgByStatic(@NotNull final Blackhole bh) {
        final Table result = ExecutionContext.getContext().getUpdateGraph().sharedLock()
                .computeLocked(() -> table.avgBy(keyColumnNames));
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table avgByIncremental(@NotNull final Blackhole bh) {
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock()
                            .computeLocked(() -> t.avgBy(keyColumnNames));
                }, table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    public static void main(String[] args) {
        final int heapGb = 12;
        final String benchName = SumByBenchmark.class.getSimpleName();
        BenchUtil.run(heapGb, SumByBenchmark.class, "sumByStatic", "sumByIncremental");
    }
}
