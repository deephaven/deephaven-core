//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.api.agg.spec.AggSpec;
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
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@SuppressWarnings("unused")
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 1)
@Timeout(time = 45)
@Fork(1)
public class PercentileByBenchmark {
    private TableBenchmarkState state;

    @Param({"Intraday"})
    private String tableType;

    @Param({"String", "None"})
    private String keyType;

    @Param({"false"})
    private boolean grouped;

    @Param({"1000000", "10000000"})
    private int size;

    @Param({"0", "10", "10000"})
    private int keyCount;

    @Param({"long", "double", "BigDecimal"})
    private String dataType;

    @Param({"median", "normal", "tdigest"})
    private String percentileMode;

    private Table table;

    private String keyName;
    private String[] keyColumnNames;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        TestExecutionContext.createForUnitTests().open();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
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
        keyColumnNames = keyCount > 0 ? keyName.split(",") : ArrayTypeUtils.EMPTY_STRING_ARRAY;

        if (dataType.equals("BigDecimal") && percentileMode.equals("tdigest")) {
            throw new UnsupportedOperationException("tdigest does not support BigDecimal");
        }
        switch (dataType) {
            case "long":
                builder.addColumn(BenchmarkTools.numberCol("Value", long.class));
                break;
            case "double":
            case "BigDecimal":
                builder.addColumn(BenchmarkTools.numberCol("Value", double.class, -1_000_000, 1_000_000));
                break;
            default:
                throw new IllegalArgumentException("Unknown dataType: " + dataType);
        }

        if (grouped) {
            ((PersistentBenchmarkTableBuilder) builder).addGroupingColumns(keyName);
        }

        final BenchmarkTable bmt = builder
                .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());

        final Table coalesced = bmt.getTable().coalesce().dropColumns("PartCol");
        table = dataType.equals("BigDecimal")
                ? coalesced.update("Value=java.math.BigDecimal.valueOf(Value)")
                : coalesced;
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
    public Table percentileByStatic(@NotNull final Blackhole bh) {
        final Function<Table, Table> fut = getFunction();
        final Table result =
                ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(() -> fut.apply(table));
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @NotNull
    private Function<Table, Table> getFunction() {
        final Function<Table, Table> fut;
        if (percentileMode.equals("median")) {
            fut = t -> t.aggAllBy(AggSpec.median(), keyColumnNames);
        } else if (percentileMode.equals("normal")) {
            fut = t -> t.aggAllBy(AggSpec.percentile(0.99), keyColumnNames);
        } else if (percentileMode.equals("tdigest")) {
            fut = (t) -> t.aggAllBy(AggSpec.approximatePercentile(0.99, 100.0), keyColumnNames);
        } else {
            throw new IllegalArgumentException("Bad mode: " + percentileMode);
        }
        return fut;
    }

    @Benchmark
    public Table percentileByIncremental(@NotNull final Blackhole bh) {
        final Function<Table, Table> fut = getFunction();
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock()
                            .computeLocked(() -> fut.apply(t));
                }, table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @Benchmark
    public Table percentileByRolling(@NotNull final Blackhole bh) {
        final Function<Table, Table> fut = getFunction();
        final Table result = IncrementalBenchmark.rollingBenchmark(
                (t) -> {
                    return ExecutionContext.getContext().getUpdateGraph().sharedLock()
                            .computeLocked(() -> fut.apply(t));
                }, table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    public static void main(String[] args) throws RunnerException {
        final int heapGb = 12;
        BenchUtil.run(heapGb, PercentileByBenchmark.class);
    }
}
