//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.SafeCloseable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmark.engine.AsOfJoinBuildBenchmark.LEFT_MULTIPLIER;
import static io.deephaven.benchmark.engine.AsOfJoinBuildBenchmark.RIGHT_MULTIPLIER;
import static io.deephaven.benchmark.engine.AsOfJoinBuildBenchmark.sideTable;

/**
 * Measures one update cycle of an as-of join in which each refreshing input removes its last {@code replacedRows} rows
 * and adds them back at the same row keys, so the join looks up each changed row's key and restamps the left rows that
 * the change affects:
 * <ul>
 * <li>{@link #leftRowsReplaced()}: the left refreshes against a static right table.</li>
 * <li>{@link #rightRowsReplaced()}: the right refreshes against a static left table.</li>
 * <li>{@link #bothRowsReplaced()}: both refresh, and both replace rows in the same cycle.</li>
 * </ul>
 * The tables are those of {@link AsOfJoinBuildBenchmark}: each side's keys are a scrambled function of the row key
 * modulo {@code keyCount}, scrambled differently on the two sides, and each row's stamp is its row key. The columns
 * compute their values from the row key, so removing and re-adding rows changes no data and the measured cycle is the
 * join's own work plus the update graph's bookkeeping.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(2)
public class AsOfJoinUpdateBenchmark {

    @Param({"1000000"})
    private int leftRows;

    @Param({"1000000"})
    private int rightRows;

    @Param({"100000", "1000000", "10000000"})
    private int keyCount;

    /** The number of rows each refreshing input removes and re-adds per cycle. */
    @Param({"100000"})
    private int replacedRows;

    private SafeCloseable executionContext;
    private ControlledUpdateGraph updateGraph;
    private QueryTable left;
    private QueryTable right;
    private Table result;
    private WritableRowSet replacedLeftRows;
    private WritableRowSet replacedRightRows;

    @Setup(Level.Trial)
    public void setupEnv(final BenchmarkParams params) {
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        // build only the join that this benchmark method updates
        final String benchmark = params.getBenchmark();
        final boolean leftRefreshing = !benchmark.endsWith(".rightRowsReplaced");
        final boolean rightRefreshing = !benchmark.endsWith(".leftRowsReplaced");
        left = sideTable("L", leftRows, LEFT_MULTIPLIER, keyCount, leftRefreshing);
        right = sideTable("R", rightRows, RIGHT_MULTIPLIER, keyCount, rightRefreshing);
        result = updateGraph.sharedLock().computeLocked(() -> left.aj(right, "LK=RK,LS>=RS", "RS"));

        // the last rows of each table, so that each replaced block ends at its table's last row key
        replacedLeftRows = RowSetFactory.fromRange(leftRows - replacedRows, leftRows - 1);
        replacedRightRows = RowSetFactory.fromRange(rightRows - replacedRows, rightRows - 1);
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        replacedLeftRows.close();
        replacedRightRows.close();
        executionContext.close();
    }

    @Benchmark
    public Table leftRowsReplaced() {
        updateGraph.runWithinUnitTestCycle(() -> replace(left, replacedLeftRows));
        return result;
    }

    @Benchmark
    public Table rightRowsReplaced() {
        updateGraph.runWithinUnitTestCycle(() -> replace(right, replacedRightRows));
        return result;
    }

    @Benchmark
    public Table bothRowsReplaced() {
        updateGraph.runWithinUnitTestCycle(() -> {
            replace(left, replacedLeftRows);
            replace(right, replacedRightRows);
        });
        return result;
    }

    private static void replace(final QueryTable table, final WritableRowSet rows) {
        // the columns are computed from the row key, so removing and re-adding the same rows changes no data
        table.notifyListeners(rows.copy(), rows.copy(), RowSetFactory.empty());
    }

    public static void main(String[] args) {
        BenchUtil.run(AsOfJoinUpdateBenchmark.class);
    }
}
