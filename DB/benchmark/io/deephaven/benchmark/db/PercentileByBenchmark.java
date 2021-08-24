package io.deephaven.benchmark.db;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.by.*;
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

    @Param({"1"})
    private int valueCount;

    @Param({"normal", "tdigest"})
    private String percentileMode;

    private Table table;

    private String keyName;
    private String[] keyColumnNames;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        QueryTable.setMemoizeResults(false);

        final BenchmarkTableBuilder builder;

        if (keyCount == 0) {
            if (!"None".equals(keyType)) {
                throw new UnsupportedOperationException(
                    "Zero Key can only be run with keyType == None");
            }
        } else {
            if ("None".equals(keyType)) {
                throw new UnsupportedOperationException(
                    "keyType == None can only be run with keyCount==0");
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
                    throw new UnsupportedOperationException(
                        "Can not run this benchmark combination.");
                }
                break;

            default:
                throw new IllegalStateException("Table type must be Historical or Intraday");
        }

        builder.setSeed(0xDEADBEEF)
            .addColumn(BenchmarkTools.stringCol("PartCol", 1, 5, 7, 0xFEEDBEEF));

        final EnumStringColumnGenerator stringKey =
            (EnumStringColumnGenerator) BenchmarkTools.stringCol("KeyString", keyCount, 6, 6,
                0xB00FB00F, EnumStringColumnGenerator.Mode.Rotate);
        final ColumnGenerator intKey = BenchmarkTools.seqNumberCol("KeyInt", int.class, 0, 1,
            keyCount, SequentialNumColumnGenerator.Mode.RollAtLimit);

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
                    throw new UnsupportedOperationException(
                        "Can not run this benchmark combination.");
                }
                break;
            case "None":
                keyName = "<bad key name for None>";
                if (grouped) {
                    throw new UnsupportedOperationException(
                        "Can not run this benchmark combination.");
                }
                break;
            default:
                throw new IllegalStateException("Unknown KeyType: " + keyType);
        }
        keyColumnNames =
            keyCount > 0 ? keyName.split(",") : CollectionUtil.ZERO_LENGTH_STRING_ARRAY;

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
                throw new IllegalArgumentException(
                    "Can not initialize with " + valueCount + " values.");
        }

        if (grouped) {
            ((PersistentBenchmarkTableBuilder) builder).addGroupingColumns(keyName);
        }

        final BenchmarkTable bmt = builder
            .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()),
            params.getWarmup().getCount());

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
    public Table percentileByStatic(@NotNull final Blackhole bh) {
        final Function<Table, Table> fut = getFunction();
        final Table result =
            LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> fut.apply(table));
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    @NotNull
    private Function<Table, Table> getFunction() {
        final Function<Table, Table> fut;
        if (percentileMode.equals("normal")) {
            fut = (t) -> t.by(new PercentileByStateFactoryImpl(0.99), keyColumnNames);
        } else if (percentileMode.equals("tdigest")) {
            fut = (t) -> {
                final NonKeyColumnAggregationFactory aggregationContextFactory =
                    new NonKeyColumnAggregationFactory(
                        (type, resultName, exposeInternalColumns) -> new TDigestPercentileOperator(
                            type, 100.0, 0.99, resultName));
                return ChunkedOperatorAggregationHelper.aggregation(aggregationContextFactory,
                    (QueryTable) t, SelectColumnFactory.getExpressions(keyColumnNames));
            };
        } else {
            throw new IllegalArgumentException("Bad mode: " + percentileMode);
        }
        return fut;
    }

    @Benchmark
    public Table percentileByIncremental(@NotNull final Blackhole bh) {
        final Function<Table, Table> fut = getFunction();
        final Table result = IncrementalBenchmark.incrementalBenchmark(
            (t) -> LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> fut.apply(t)), table);
        bh.consume(result);
        return state.setResult(TableTools.emptyTable(0));
    }

    public static void main(String[] args) throws RunnerException {
        final int heapGb = 12;
        BenchUtil.run(heapGb, PercentileByBenchmark.class);
    }
}
