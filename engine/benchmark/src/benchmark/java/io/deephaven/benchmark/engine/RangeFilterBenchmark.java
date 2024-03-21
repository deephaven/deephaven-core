//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 3, time = 5)
@Timeout(time = 10)
@Fork(1)
public class RangeFilterBenchmark {
    private TableBenchmarkState state;

    @Param({"D1", "F1", "I1", "Timestamp"})
    private String filterCol;

    @Param({"Intraday"})
    private String tableType;

    @Param({"10000000"})
    private int tableSize;

    @Param({"100", "50"})
    private int sparsity;

    @Param({"0", "10", "50", "90", "100"})
    private int selectivity;

    private Table inputTable;
    private AbstractRangeFilter rangeFilter;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        TestExecutionContext.createForUnitTests().open();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();

        final BenchmarkTableBuilder builder;
        final int actualSize = BenchmarkTools.sizeWithSparsity(tableSize, sparsity);

        switch (tableType) {
            case "Historical":
                builder = BenchmarkTools.persistentTableBuilder("Carlos", actualSize)
                        .setPartitioningFormula("${autobalance_single}")
                        .setPartitionCount(10);
                break;
            case "Intraday":
                builder = BenchmarkTools.persistentTableBuilder("Carlos", actualSize);
                break;

            default:
                throw new IllegalStateException("Table type must be Historical or Intraday");
        }

        builder.setSeed(0xDEADBEEF)
                .addColumn(BenchmarkTools.stringCol("PartCol", 4, 5, 7, 0xFEEDBEEF));

        final Instant startTime = DateTimeUtils.parseInstant("2019-01-01T12:00:00 NY");
        final Instant endTime = DateTimeUtils.parseInstant("2019-12-31T12:00:00 NY");

        switch (filterCol) {
            case "D1":
                builder.addColumn(BenchmarkTools.numberCol("D1", double.class, -10e6, 10e6));
                break;
            case "F1":
                builder.addColumn(BenchmarkTools.numberCol("F1", float.class, -10e6f, 10e6f));
                break;
            case "L1":
                builder.addColumn(BenchmarkTools.numberCol("L1", long.class, -10_000_000, 10_000_000));
                break;
            case "I1":
                builder.addColumn(BenchmarkTools.numberCol("I1", int.class, -10_000_000, 10_000_000));
                break;
            case "Timestamp":
                builder.addColumn(BenchmarkTools.instantCol("Timestamp", startTime, endTime));
                break;
        }

        if (filterCol.equals("Timestamp")) {
            final Instant lowerBound, upperBound;
            if (selectivity == 100) {
                upperBound = endTime;
                lowerBound = startTime;
            } else if (selectivity == 0) {
                lowerBound = DateTimeUtils.plus(endTime, 1000_000_000L);
                upperBound = DateTimeUtils.plus(lowerBound, 1000_000_00L);
            } else {
                final long midpoint = (DateTimeUtils.epochNanos(startTime) + DateTimeUtils.epochNanos(endTime)) / 2;
                final long range = (DateTimeUtils.epochNanos(endTime) - DateTimeUtils.epochNanos(startTime));
                lowerBound = DateTimeUtils.epochNanosToInstant(midpoint - (long) (range * (selectivity / 100.0)));
                upperBound = DateTimeUtils.epochNanosToInstant(midpoint + (long) (range * (selectivity / 100.0)));
            }

            assert lowerBound != null;
            assert upperBound != null;

            rangeFilter = new InstantRangeFilter(filterCol, lowerBound, upperBound);
        } else {
            final double lowerBound, upperBound;
            if (selectivity == 100) {
                upperBound = 10e6;
                lowerBound = -upperBound;
            } else if (selectivity == 0) {
                upperBound = -11e6;
                lowerBound = -12e6;
            } else {
                upperBound = (selectivity / 100.0) * 10e6;
                lowerBound = -upperBound;
            }

            rangeFilter = new DoubleRangeFilter(filterCol, lowerBound, upperBound);
        }

        final BenchmarkTable bmTable = builder.build();
        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());
        inputTable = applySparsity(bmTable.getTable(), tableSize, sparsity, 0).coalesce();
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

    private <R> R incrementalBenchmark(Function<Table, R> function) {
        final long sizePerStep = Math.max(inputTable.size() / 10, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
                new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table filtered = inputTable.where(incrementalReleaseFilter);

        final R result = function.apply(filtered);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();

        while (filtered.size() < inputTable.size()) {
            updateGraph.runWithinUnitTestCycle(incrementalReleaseFilter::run);
        }

        return result;
    }

    @Benchmark
    public Table incrementalFilter() {
        final Table result = incrementalBenchmark((Table t) -> t.where(rangeFilter));
        return state.setResult(result);
    }

    @Benchmark
    public Table staticRangeFilter() {
        return state.setResult(inputTable.where(rangeFilter)).coalesce();
    }

    public static void main(String[] args) {
        BenchUtil.run(RangeFilterBenchmark.class);
    }
}
