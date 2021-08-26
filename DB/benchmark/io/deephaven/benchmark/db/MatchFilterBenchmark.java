package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.select.*;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 10, time = 5)
@Timeout(time = 10)
@Fork(1)
public class MatchFilterBenchmark {
    private TableBenchmarkState state;

    @Param({"L1", "I1", "Timestamp", "Symbol"})
    private String filterCol;

    @Param({"Intraday"})
    private String tableType;

    @Param({"10000000"})
    private int tableSize;

    @Param({"100", "50"})
    private int sparsity;

    // how many values should we try to match
    @Param({"1", "2", "3", "5", "100"})
    private int matchValues;

    private Table inputTable;
    private SelectFilter matchFilter;
    private final ResultSizeProfiler resultSizeProfiler = new ResultSizeProfiler();

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();

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

        final DBDateTime startTime = DBTimeUtils.convertDateTime("2019-01-01T12:00:00 NY");
        final DBDateTime endTime = DBTimeUtils.convertDateTime("2019-01-01T12:00:00.000001 NY");

        switch (filterCol) {
            case "L1":
                builder.addColumn(BenchmarkTools.numberCol("L1", long.class, 0, 1000));
                break;
            case "I1":
                builder.addColumn(BenchmarkTools.numberCol("I1", int.class, 0, 1000));
                break;
            case "Symbol":
                builder.addColumn(BenchmarkTools.stringCol("Symbol", 1000, 1, 10, 0));
                break;
            case "Timestamp":
                builder.addColumn(BenchmarkTools.dateCol("Timestamp", startTime, endTime));
                break;
        }

        final BenchmarkTable bmTable = builder.build();
        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());
        inputTable = applySparsity(bmTable.getTable(), tableSize, sparsity, 0).coalesce();


        final List<Object> values = new ArrayList<>();
        if (filterCol.equals("Timestamp")) {
            for (int ii = 0; ii < matchValues; ++ii) {
                values.add(DBTimeUtils.plus(startTime, ii));
            }
        } else if (filterCol.equals("Symbol")) {
            inputTable.selectDistinct("Symbol").head(matchValues).columnIterator("Symbol")
                    .forEachRemaining(values::add);
        } else {
            for (int ii = 0; ii < matchValues; ++ii) {
                values.add(ii);
            }
        }
        matchFilter = new MatchFilter(filterCol, values.toArray());
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
        ResultSizeProfiler.setResultSize(state.resultSize());
        state.processResult(params);
    }

    private <R> R incrementalBenchmark(Function<Table, R> function) {
        final long sizePerStep = Math.max(inputTable.size() / 10, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
                new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table filtered = inputTable.where(incrementalReleaseFilter);

        final R result = function.apply(filtered);

        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        while (filtered.size() < inputTable.size()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);
        }

        return result;
    }

    @Benchmark
    public Table incrementalFilter() {
        final Table result = incrementalBenchmark((Table t) -> t.where(matchFilter));
        return state.setResult(result);
    }

    @Benchmark
    public Table staticRangeFilter() {
        return state.setResult(inputTable.where(matchFilter)).coalesce();
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(MatchFilterBenchmark.class);
    }

}
