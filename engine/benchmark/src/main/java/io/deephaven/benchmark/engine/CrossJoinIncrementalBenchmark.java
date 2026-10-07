//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.base.verify.Require;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
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

/**
 * Measures one update cycle of a keyed cross join ({@code join}) whose inputs both refresh, as their keys turn over.
 * <p>
 * Each input holds a window of {@code rows} consecutive logical rows. Each cycle removes the oldest {@code churnRows}
 * rows from both inputs and appends as many new ones, so the window slides forward. Logical row {@code n} has the row
 * key {@code n % (2 * rows)}, so the row keys cycle through a fixed range and anything the join indexes by row key
 * stays the same size, and the key {@code (n / rowsPerKey) % (keySpaceMultiple * rows / rowsPerKey)}. Each key's rows
 * are adjacent, so a removed key leaves both inputs entirely, and the key space is {@code keySpaceMultiple} times the
 * live keys, so a key returns once the window has moved through the whole key space. A join that keeps the state of
 * every key it has seen holds {@code keySpaceMultiple} times the live keys once warm; one that releases dead keys holds
 * only the live ones, but removes and re-adds keys as they come and go.
 * <p>
 * The key columns compute their value from the row key and the window position, so the measured cycle is the join's own
 * work plus the update graph's bookkeeping. At the end of each trial, the benchmark prints the heap that survives a
 * collection while the join is reachable, which shows the state the join retains.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class CrossJoinIncrementalBenchmark {

    /**
     * The first logical row of the window, now and at the start of the cycle.
     */
    private static final class Window {
        private final long ringSize;
        private long firstRow;
        private long prevFirstRow;

        private Window(final long ringSize) {
            this.ringSize = ringSize;
        }

        /**
         * @return the logical row of a row key in the window that starts at {@code first}
         */
        private long logicalRow(final long first, final long rowKey) {
            return first + Math.floorMod(rowKey - first, ringSize);
        }
    }

    /**
     * A long key column whose value at a row key is {@code (logicalRow / rowsPerKey) % keySpace}.
     */
    private static final class WindowKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final Window window;
        private final long rowsPerKey;
        private final long keySpace;

        private WindowKeySource(final Window window, final long rowsPerKey, final long keySpace) {
            super(long.class);
            this.window = window;
            this.rowsPerKey = rowsPerKey;
            this.keySpace = keySpace;
        }

        @Override
        public long getLong(final long rowKey) {
            return (window.logicalRow(window.firstRow, rowKey) / rowsPerKey) % keySpace;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return (window.logicalRow(window.prevFirstRow, rowKey) / rowsPerKey) % keySpace;
        }

        @Override
        public boolean isImmutable() {
            return false;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    }

    @Param({"1000000"})
    private int rows;

    @Param({"2"})
    private int rowsPerKey;

    @Param({"10000"})
    private int churnRows;

    @Param({"2", "16"})
    private int keySpaceMultiple;

    private ControlledUpdateGraph updateGraph;
    private SafeCloseable executionContext;
    private Window window;
    private QueryTable left;
    private QueryTable right;
    private Table result;

    @Setup(Level.Trial)
    public void setupEnv() {
        // a cycle's removed and added rows are each one range of row keys
        Require.eqZero(2L * rows % churnRows, "2 * rows % churnRows");
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        window = new Window(2L * rows);
        final long keySpace = (long) keySpaceMultiple * rows / rowsPerKey;
        left = windowTable("LK", keySpace);
        right = windowTable("RK", keySpace);
        result = updateGraph.sharedLock().computeLocked(() -> left.join(right, "LK=RK"));
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        for (int ii = 0; ii < 3; ++ii) {
            System.gc();
        }
        final Runtime runtime = Runtime.getRuntime();
        final long retainedBytes = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("CrossJoinIncrementalBenchmark retained heap: keySpaceMultiple=" + keySpaceMultiple
                + ", rows=" + rows + ", MiB=" + (retainedBytes >> 20) + ", result size=" + result.size());
        executionContext.close();
    }

    private QueryTable windowTable(final String name, final long keySpace) {
        final QueryTable table = new QueryTable(RowSetFactory.flat(rows).toTracking(),
                Map.of(name, new WindowKeySource(window, rowsPerKey, keySpace)));
        table.setRefreshing(true);
        return table;
    }

    @Benchmark
    public Table slideWindow() {
        final long removedStart = Math.floorMod(window.firstRow, window.ringSize);
        final long addedStart = Math.floorMod(window.firstRow + rows, window.ringSize);
        updateGraph.runWithinUnitTestCycle(() -> {
            window.firstRow += churnRows;
            slide(left, removedStart, addedStart);
            slide(right, removedStart, addedStart);
        });
        window.prevFirstRow = window.firstRow;
        return result;
    }

    private void slide(final QueryTable table, final long removedStart, final long addedStart) {
        final WritableRowSet removed = RowSetFactory.fromRange(removedStart, removedStart + churnRows - 1);
        final WritableRowSet added = RowSetFactory.fromRange(addedStart, addedStart + churnRows - 1);
        table.getRowSet().writableCast().update(added, removed);
        table.notifyListeners(added, removed, RowSetFactory.empty());
    }

    public static void main(String[] args) {
        BenchUtil.run(CrossJoinIncrementalBenchmark.class);
    }
}
