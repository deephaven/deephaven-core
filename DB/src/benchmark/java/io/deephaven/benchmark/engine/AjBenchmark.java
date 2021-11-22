package io.deephaven.benchmark.engine;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.metrics.MetricsManager;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.benchmarking.generator.FuzzyNumColumnGenerator;
import io.deephaven.benchmarking.generator.SequentialNumColumnGenerator;
import io.deephaven.benchmarking.impl.PersistentBenchmarkTableBuilder;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 5, time = 5)
@Timeout(time = 12)
@Fork(value = 1)
public class AjBenchmark {
    private TableBenchmarkState state;

    @Param({"HistoricalOrdered"})
    private String tableType;

    @Param({"false"})
    private boolean fingerprint;

    @Param({"false"})
    private boolean grouped;

    @Param({"100000"})
    private int leftSize;

    @Param({"1000000"})
    private int rightSize;

    @Param({"100000"})
    private int buckets;

    private Table rightTable;
    private Table leftTable;

    private String joinKeyName;
    private BenchmarkTable bmRight;
    private BenchmarkTable bmLeft;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        System.out.println("Setup started: " + new Date());

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();

        final BenchmarkTableBuilder rightBuilder;
        final BenchmarkTableBuilder leftBuilder;

        switch (tableType) {
            case "Historical":
            case "HistoricalOrdered":
                rightBuilder = BenchmarkTools.persistentTableBuilder("Carlos", rightSize)
                        .setPartitioningFormula("${autobalance_single}")
                        .setPartitionCount(10);
                leftBuilder = BenchmarkTools.persistentTableBuilder("Karl", leftSize)
                        .setPartitioningFormula("${autobalance_single}")
                        .setPartitionCount(10);
                break;
            case "Intraday":
                rightBuilder = BenchmarkTools.persistentTableBuilder("Carlos", rightSize);
                leftBuilder = BenchmarkTools.persistentTableBuilder("Karl", leftSize);
                if (grouped) {
                    throw new UnsupportedOperationException("Can not run this benchmark combination.");
                }
                break;

            default:
                throw new IllegalStateException("Table type must be Historical or Intraday");
        }

        rightBuilder.setSeed(0xDEADBEEF).addColumn(BenchmarkTools.stringCol("PartCol", 1, 5, 7, 0xFEEDBEEF));
        leftBuilder.setSeed(0xDEADBEEF).addColumn(BenchmarkTools.stringCol("PartCol", 1, 5, 7, 0xFEEDBEEF));

        if (buckets > 0) {
            final ColumnGenerator intJoinKey = BenchmarkTools.numberCol("JInt", int.class, 0, buckets);

            rightBuilder.addColumn(intJoinKey);
            leftBuilder.addColumn(intJoinKey);
            joinKeyName = intJoinKey.getName();
        } else {
            joinKeyName = "InvalidKeyWithoutBuckets";
        }

        final ColumnGenerator rightStampColumn = BenchmarkTools.seqNumberCol("RightStamp", int.class, 0, 10);
        final ColumnGenerator leftStampColumn;
        if (tableType.equals("HistoricalOrdered")) {
            leftStampColumn =
                    new FuzzyNumColumnGenerator<>(int.class, "LeftStamp", 0, (rightSize * 10) / (double) leftSize,
                            rightSize * 10, 0.01, SequentialNumColumnGenerator.Mode.NoLimit);
        } else {
            leftStampColumn = BenchmarkTools.numberCol("LeftStamp", int.class, 0, rightSize * 10);
        }

        if (grouped) {
            ((PersistentBenchmarkTableBuilder) leftBuilder).addGroupingColumns(joinKeyName);
            ((PersistentBenchmarkTableBuilder) rightBuilder).addGroupingColumns(joinKeyName);
        }

        bmRight = rightBuilder
                .addColumn(rightStampColumn)
                .addColumn(BenchmarkTools.numberCol("RightSentinel", long.class))
                .build();

        bmLeft = leftBuilder
                .addColumn(leftStampColumn)
                .addColumn(BenchmarkTools.numberCol("LeftSentinel", long.class))
                .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());

        rightTable = bmRight.getTable().coalesce().dropColumns("PartCol");
        leftTable = bmLeft.getTable().coalesce().dropColumns("PartCol");

        System.out.println("Setup completed: " + new Date());
    }

    @TearDown(Level.Trial)
    public void finishTrial() {
        try {
            state.logOutput();
        } catch (IOException e) {
            e.printStackTrace();
        }
        bmLeft.cleanup();
        bmRight.cleanup();
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        state.init();
    }

    @TearDown(Level.Iteration)
    public void finishIteration(BenchmarkParams params) throws IOException {
        state.processResult(params);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        MetricsManager.instance.bluntResetAllCounters();
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        MetricsManager.instance.update("bench invocation");
    }

    @Benchmark
    public Table ajStatic(Blackhole bh) {
        if (buckets == 0) {
            throw new UnsupportedOperationException("Buckets must be positive!");
        }
        final Table result = UpdateGraphProcessor.DEFAULT.sharedLock()
                .computeLocked(() -> leftTable.aj(rightTable, joinKeyName + ",LeftStamp=RightStamp", "RightSentinel"));
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajLeftIncremental(Blackhole bh) {
        if (buckets == 0) {
            throw new UnsupportedOperationException("Buckets must be positive!");
        }
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (lt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .computeLocked(() -> lt.aj(rightTable, joinKeyName + ",LeftStamp=RightStamp", "RightSentinel")),
                leftTable);
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajLeftIncrementalSmallSteps(Blackhole bh) {
        if (buckets == 0) {
            throw new UnsupportedOperationException("Buckets must be positive!");
        }
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (lt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .computeLocked(() -> lt.aj(rightTable, joinKeyName + ",LeftStamp=RightStamp", "RightSentinel")),
                leftTable, 100);
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajLeftIncrementalTinySteps(Blackhole bh) {
        if (buckets == 0) {
            throw new UnsupportedOperationException("Buckets must be positive!");
        }
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (lt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .computeLocked(() -> lt.aj(rightTable, joinKeyName + ",LeftStamp=RightStamp", "RightSentinel")),
                leftTable, 1000);
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajRightIncremental(Blackhole bh) {
        if (buckets == 0) {
            throw new UnsupportedOperationException("Buckets must be positive!");
        }
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (rt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .computeLocked(() -> leftTable.aj(rt, joinKeyName + ",LeftStamp=RightStamp", "RightSentinel")),
                rightTable);
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajZkStatic(Blackhole bh) {
        if (buckets != 0) {
            throw new UnsupportedOperationException("Zero key should have zero buckets!");
        }
        final Table result = UpdateGraphProcessor.DEFAULT.sharedLock()
                .computeLocked(() -> leftTable.aj(rightTable, "LeftStamp=RightStamp", "RightSentinel"));
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajZkLeftIncremental(Blackhole bh) {
        if (buckets != 0) {
            throw new UnsupportedOperationException("Zero key should have zero buckets!");
        }
        final Table result =
                IncrementalBenchmark.incrementalBenchmark(
                        (lt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                                .computeLocked(() -> lt.aj(rightTable, "LeftStamp=RightStamp", "RightSentinel")),
                        leftTable);
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajZkRightIncremental(Blackhole bh) {
        if (buckets != 0) {
            throw new UnsupportedOperationException("Zero key should have zero buckets!");
        }
        final Table result =
                IncrementalBenchmark.incrementalBenchmark(
                        (rt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                                .computeLocked(() -> leftTable.aj(rt, "LeftStamp=RightStamp", "RightSentinel")),
                        rightTable);
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajZkIncremental(Blackhole bh) {
        if (buckets != 0) {
            throw new UnsupportedOperationException("Zero key should have zero buckets!");
        }
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (lt, rt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .computeLocked(() -> lt.aj(rt, "LeftStamp=RightStamp", "RightSentinel")),
                leftTable, rightTable);
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajZkIncrementalStartup(Blackhole bh) {
        if (buckets != 0) {
            throw new UnsupportedOperationException("Zero key should have zero buckets!");
        }
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (lt, rt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .computeLocked(() -> lt.aj(rt, "LeftStamp=RightStamp", "RightSentinel")),
                leftTable, rightTable, 0.95, 1);
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajZkIncrementalSmallSteps(Blackhole bh) {
        if (buckets != 0) {
            throw new UnsupportedOperationException("Zero key should have zero buckets!");
        }
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (lt, rt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .computeLocked(() -> lt.aj(rt, "LeftStamp=RightStamp", "RightSentinel")),
                leftTable, rightTable, 0.1, 100);
        return doFingerPrint(result, bh);
    }


    @Benchmark
    public Table ajIncrementalSmallSteps(Blackhole bh) {
        if (buckets == 0) {
            throw new UnsupportedOperationException("Buckets must be positive!");
        }
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (lt, rt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .computeLocked(() -> lt.aj(rt, joinKeyName + ",LeftStamp=RightStamp", "RightSentinel")),
                leftTable, rightTable, 0.1, 100);
        return doFingerPrint(result, bh);
    }

    @Benchmark
    public Table ajIncremental(Blackhole bh) {
        if (buckets == 0) {
            throw new UnsupportedOperationException("Buckets must be positive!");
        }
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (lt, rt) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .computeLocked(() -> lt.aj(rt, joinKeyName + ",LeftStamp=RightStamp", "RightSentinel")),
                leftTable, rightTable);
        return doFingerPrint(result, bh);
    }

    private Table doFingerPrint(Table result, Blackhole bh) {
        if (fingerprint) {
            return state.setResult(result);
        } else {
            bh.consume(result);
            return state.setResult(TableTools.emptyTable(0));
        }
    }

    public static void main(String[] args) {
        final int heapGb = 7;
        BenchUtil.run(heapGb, AjBenchmark.class);
    }
}
