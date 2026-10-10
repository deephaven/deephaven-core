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
 * Measures one update cycle of a natural join whose right keys turn over.
 * <p>
 * The right input holds a window of {@code rightRows} consecutive logical rows, logical row {@code n} having the key
 * {@code n % keySpace}, so its keys are unique. Each cycle removes the oldest {@code churnRows} right rows and appends
 * as many new ones, so the window slides forward and a key returns once the window has moved through the whole key
 * space, which is {@code keySpaceMultiple} times the live keys. Logical row {@code n} has the row key
 * {@code n % (2 * rightRows)}, so the row keys cycle through a fixed range.
 * <ul>
 * <li>With {@code ticking} {@code both}, the left input slides in step, holding {@code leftRowsPerKey} rows for each
 * live right key, so a key leaves both sides at once and the join removes it.</li>
 * <li>With {@code ticking} {@code right}, the left input is static and holds {@code leftRowsPerKey} rows for each key
 * of the whole key space, so every right row finds its key and changes the left rows' redirection.</li>
 * </ul>
 * The key columns compute their values from the row key and the window position, so the measured cycle is the join's
 * own work plus the update graph's bookkeeping.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class NaturalJoinChurnBenchmark {

    /**
     * The first logical row of the right window, now and at the start of the cycle.
     */
    private static final class Window {
        private long firstRow;
        private long prevFirstRow;
    }

    /**
     * A long key column over a ring of {@code ringSize} row keys, each holding logical row
     * {@code first * rowsPerKey + ((rowKey - first * rowsPerKey) mod ringSize)}, whose key is
     * {@code (logicalRow / rowsPerKey) % keySpace}.
     */
    private static final class WindowKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final Window window;
        private final long ringSize;
        private final long rowsPerKey;
        private final long keySpace;

        private WindowKeySource(final Window window, final long ringSize, final long rowsPerKey,
                final long keySpace) {
            super(long.class);
            this.window = window;
            this.ringSize = ringSize;
            this.rowsPerKey = rowsPerKey;
            this.keySpace = keySpace;
        }

        private long key(final long first, final long rowKey) {
            final long firstLogical = first * rowsPerKey;
            final long logicalRow = firstLogical + Math.floorMod(rowKey - firstLogical, ringSize);
            return (logicalRow / rowsPerKey) % keySpace;
        }

        @Override
        public long getLong(final long rowKey) {
            return key(window.firstRow, rowKey);
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return key(window.prevFirstRow, rowKey);
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

    /**
     * A static long key column whose value at a row key is {@code (rowKey / rowsPerKey) % keySpace}.
     */
    private static final class StaticKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long rowsPerKey;
        private final long keySpace;

        private StaticKeySource(final long rowsPerKey, final long keySpace) {
            super(long.class);
            this.rowsPerKey = rowsPerKey;
            this.keySpace = keySpace;
        }

        @Override
        public long getLong(final long rowKey) {
            return (rowKey / rowsPerKey) % keySpace;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return getLong(rowKey);
        }

        @Override
        public boolean isImmutable() {
            return true;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    }

    @Param({"both", "right"})
    private String ticking;

    @Param({"500000"})
    private int rightRows;

    @Param({"2"})
    private int leftRowsPerKey;

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
        Require.eqZero(2L * rightRows % churnRows, "2 * rightRows % churnRows");
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        window = new Window();
        final long keySpace = (long) keySpaceMultiple * rightRows;
        right = windowTable("RK", rightRows, 1, keySpace);
        if (ticking.equals("both")) {
            left = windowTable("LK", rightRows * leftRowsPerKey, leftRowsPerKey, keySpace);
        } else {
            Require.eqTrue(ticking.equals("right"), "ticking.equals(\"right\")");
            left = new QueryTable(RowSetFactory.flat(keySpace * leftRowsPerKey).toTracking(),
                    Map.of("LK", new StaticKeySource(leftRowsPerKey, keySpace)));
        }
        result = updateGraph.sharedLock().computeLocked(() -> left.naturalJoin(right, "LK=RK"));
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        executionContext.close();
    }

    private QueryTable windowTable(final String name, final long size, final long rowsPerKey, final long keySpace) {
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(),
                Map.of(name, new WindowKeySource(window, 2 * size, rowsPerKey, keySpace)));
        table.setRefreshing(true);
        return table;
    }

    @Benchmark
    public Table slideWindow() {
        updateGraph.runWithinUnitTestCycle(() -> {
            final long first = window.firstRow;
            window.firstRow += churnRows;
            slide(right, first, rightRows, 1);
            if (ticking.equals("both")) {
                slide(left, first, rightRows, leftRowsPerKey);
            }
        });
        window.prevFirstRow = window.firstRow;
        return result;
    }

    /**
     * Remove the oldest {@code churnRows * rowsPerKey} rows of a window of {@code windowRows * rowsPerKey} and append
     * as many, in the ring of twice the window's row keys.
     */
    private void slide(final QueryTable table, final long first, final long windowRows, final long rowsPerKey) {
        final long ringSize = 2 * windowRows * rowsPerKey;
        final long removedStart = Math.floorMod(first * rowsPerKey, ringSize);
        final long addedStart = Math.floorMod((first + windowRows) * rowsPerKey, ringSize);
        final long count = churnRows * rowsPerKey;
        final WritableRowSet removed = RowSetFactory.fromRange(removedStart, removedStart + count - 1);
        final WritableRowSet added = RowSetFactory.fromRange(addedStart, addedStart + count - 1);
        table.getRowSet().writableCast().update(added, removed);
        table.notifyListeners(added, removed, RowSetFactory.empty());
    }

    public static void main(String[] args) {
        BenchUtil.run(NaturalJoinChurnBenchmark.class);
    }
}
