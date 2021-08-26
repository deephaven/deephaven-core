package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.utils.metrics.MetricsManager;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.benchmarking.generator.EnumStringColumnGenerator;
import io.deephaven.benchmarking.generator.SequentialNumColumnGenerator;
import io.deephaven.benchmarking.impl.PersistentBenchmarkTableBuilder;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 3, time = 7)
@Timeout(time = 20)
@Fork(1)
public class NaturalJoinBenchmark {
    private TableBenchmarkState state;

    @Param({"Historical"})
    private String tableType;

    @Param({"Int"}) // "String", "Int", "Composite"})
    private String joinKeyType;

    @Param({"false"})
    private boolean leftGrouped;

    @Param({"10000000"})
    private int leftSize;

    @Param({"10000"}) // , "10"})
    private int rightSize;

    private Table rightTable;
    private Table leftTable;

    private String joinKeyName;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        final BenchmarkTableBuilder rightBuilder;
        final BenchmarkTableBuilder leftBuilder;

        switch (tableType) {
            case "Historical":
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
                if (leftGrouped) {
                    throw new UnsupportedOperationException("Can not run this benchmark combination.");
                }
                break;

            default:
                throw new IllegalStateException("Table type must be Historical or Intraday");
        }

        rightBuilder.setSeed(0xDEADBEEF).addColumn(BenchmarkTools.stringCol("PartCol", 1, 5, 7, 0xFEEDBEEF));
        leftBuilder.setSeed(0xDEADBEEF).addColumn(BenchmarkTools.stringCol("PartCol", 1, 5, 7, 0xFEEDBEEF));

        final EnumStringColumnGenerator stringJoinKey = (EnumStringColumnGenerator) BenchmarkTools.stringCol("JString",
                rightSize, 6, 6, 0xB00FB00F, EnumStringColumnGenerator.Mode.Rotate);
        final ColumnGenerator intJoinKey = BenchmarkTools.seqNumberCol("JInt", int.class, 0, 1, rightSize,
                SequentialNumColumnGenerator.Mode.RollAtLimit);

        System.out.println("Join key type: " + joinKeyType);
        switch (joinKeyType) {
            case "String":
                rightBuilder.addColumn(stringJoinKey);
                leftBuilder.addColumn(stringJoinKey);
                joinKeyName = stringJoinKey.getName();
                break;
            case "Int":
                rightBuilder.addColumn(intJoinKey);
                leftBuilder.addColumn(intJoinKey);
                joinKeyName = intJoinKey.getName();
                break;
            case "Composite":
                rightBuilder.addColumn(stringJoinKey);
                rightBuilder.addColumn(intJoinKey);
                leftBuilder.addColumn(stringJoinKey);
                leftBuilder.addColumn(intJoinKey);
                joinKeyName = stringJoinKey.getName() + "," + intJoinKey.getName();
                if (leftGrouped) {
                    throw new UnsupportedOperationException("Can not run this benchmark combination.");
                }
                break;
            default:
                throw new IllegalStateException("Unknown JoinKeyType: " + joinKeyType);
        }

        if (leftGrouped) {
            ((PersistentBenchmarkTableBuilder) leftBuilder).addGroupingColumns(joinKeyName);
        }

        final BenchmarkTable bmRight = rightBuilder
                .addColumn(BenchmarkTools.numberCol("RightSentinel", long.class))
                .build();

        final BenchmarkTable bmLeft = leftBuilder
                .addColumn(BenchmarkTools.numberCol("LeftSentinel", long.class))
                .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());

        rightTable = bmRight.getTable().coalesce().dropColumns("PartCol");
        leftTable = bmLeft.getTable().coalesce().dropColumns("PartCol");
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

    @Setup(Level.Invocation)
    public void setupInvocation() {
        MetricsManager.instance.bluntResetAllCounters();
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        MetricsManager.instance.update("bench invocation");
    }

    @Benchmark
    public Table naturalJoinStatic() {
        final Table result = LiveTableMonitor.DEFAULT.sharedLock()
                .computeLocked(() -> leftTable.naturalJoin(rightTable, joinKeyName));
        return state.setResult(result);
    }

    @Benchmark
    public Table naturalJoinIncremental() {
        final Table result = IncrementalBenchmark.incrementalBenchmark(
                (lt, rt) -> LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> lt.naturalJoin(rt, joinKeyName)),
                leftTable, rightTable);
        return state.setResult(result);
    }

    public static void main(String[] args) {
        final int heapGb = 30;
        BenchUtil.run(heapGb, NaturalJoinBenchmark.class);
    }
}
