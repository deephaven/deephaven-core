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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.LEFT;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.RIGHT;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.keySource;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.keys;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.sideRows;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.table;

/**
 * Measures one update cycle of a natural join whose refreshing left table probes a static right table: each cycle
 * removes the last {@code replacedRows} left rows and adds them back at the same row keys, so the left listener probes
 * the key of every added row and stores its redirection.
 * <p>
 * Each side holds {@code keyCount} distinct keys, of which {@code hitPercent} percent occur on the other side, as
 * {@link JoinBenchmarkKeys} builds them; so that percentage of the probed left rows find their key, at every key count.
 * The right table has one row per key, as a natural join's right keys are unique; the left has at least a million rows,
 * and one per key beyond that. Re-adding rows changes no data, so the measured cycle is the join's own work plus the
 * update graph's bookkeeping.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(2)
public class NaturalJoinLeftReprobeBenchmark {

    @Param({"100000", "1000000", "10000000"})
    private int keyCount;

    @Param({"25", "75"})
    private int hitPercent;

    /** The number of left rows removed and re-added per cycle. */
    @Param({"100000"})
    private int replacedRows;

    private SafeCloseable executionContext;
    private ControlledUpdateGraph updateGraph;
    private QueryTable left;
    private Table result;
    private WritableRowSet replacedRowSet;

    @Setup(Level.Trial)
    public void setupEnv() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        final int leftRows = sideRows(keyCount);
        left = table(leftRows, Map.of("Key", keySource(keys(leftRows, keyCount, hitPercent, LEFT))), true);
        final Table right = table(keyCount, Map.of(
                "Key", keySource(keys(keyCount, keyCount, hitPercent, RIGHT)),
                "RS", new JoinBenchmarkKeys.RowKeySource()), false);
        result = updateGraph.sharedLock().computeLocked(() -> left.naturalJoin(right, "Key", "RS"));
        // the last rows of the left table, so that the replaced block ends at the table's last row key
        replacedRowSet = RowSetFactory.fromRange(leftRows - replacedRows, leftRows - 1);
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        replacedRowSet.close();
        executionContext.close();
    }

    @Benchmark
    public Table leftRowsReplaced() {
        // the key column is in memory and unchanged, so removing and re-adding the same rows changes no data
        updateGraph.runWithinUnitTestCycle(
                () -> left.notifyListeners(replacedRowSet.copy(), replacedRowSet.copy(), RowSetFactory.empty()));
        return result;
    }

    public static void main(String[] args) {
        BenchUtil.run(NaturalJoinLeftReprobeBenchmark.class);
    }
}
