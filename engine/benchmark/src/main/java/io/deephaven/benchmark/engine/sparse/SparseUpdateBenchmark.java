//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.RowKeyColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.SparseMemoryColumnSourceFactory;
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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Measures {@code update()} into sparse array sources that hold every leaf as a full block, or into adaptive sparse
 * array sources with compact leaves, selected by {@link SparseMemoryColumnSourceFactory#USE_ADAPTIVE}, for a refreshing
 * parent whose row keys follow a pattern of {@link SparsePatterns} over a window of
 * {@link SparsePatterns#keySpace(String)} row keys, which holds at least about {@link SparsePatterns#MIN_ROWS} rows.
 * The benchmarks are:
 * <ul>
 * <li>{@link #initialUpdate()}: computing the result of the whole parent</li>
 * <li>{@link #marchCycle()}: a cycle that removes the parent's rows in its lowest cycle span of row keys and adds as
 * many beyond its highest, so that the result fills new leaves and releases old ones</li>
 * <li>{@link #modifyCycle()}: a cycle that reports every parent row within a cycle span of row keys as modified, so
 * that the result recomputes them and records their previous values</li>
 * </ul>
 * A cycle span is {@code 2^logCycleRows} times {@link SparsePatterns#keysPerRow(String)} row keys, so that each cycle
 * changes about {@code 2^logCycleRows} rows whatever the pattern. The parent's value column is computed from the row
 * key, so the measured work is the result's. The retained size that {@link SparseArrayColumnSource#estimateSize()}
 * reports for the result column is printed at the end of each trial.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class SparseUpdateBenchmark {
    private static final String FORMULA = "X = V * 2.0";

    /**
     * A long column whose value at a row key is the row key, which reports itself as mutable, as a refreshing table's
     * column would, so that {@code update()} recomputes the rows that each cycle modifies.
     */
    private static final class RowKeyValues extends RowKeyColumnSource {
        @Override
        public boolean isImmutable() {
            return false;
        }
    }

    /** Sparse array sources whose leaves are all full blocks, or whose leaves may be compact. */
    @Param({"Sparse", "Adaptive"})
    private String storage;

    @Param({"Dense", "Stride8", "Stride64", "Stride512", "Random1", "Clustered"})
    private String pattern;

    @Param({"serial", "parallel"})
    private String threads;

    @Param({"16"})
    private int logCycleRows;

    private ControlledUpdateGraph updateGraph;
    private QueryTable parent;
    private ModifiedColumnSet valuesModified;
    private Table result;
    private long windowStart;
    private long modifyStart;
    private long window;
    private long cycleKeys;

    @Setup(Level.Trial)
    public void setupTrial() {
        TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        SparseMemoryColumnSourceFactory.USE_ADAPTIVE = !storage.equals("Sparse");
        final boolean parallel = threads.equals("parallel");
        setQueryTableFlag("ENABLE_PARALLEL_SELECT_AND_UPDATE", parallel);
        // divide a cycle's rows between several jobs
        QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = 1L << 14;
        // each initialUpdate must build a new result rather than return the memoized one
        QueryTable.setMemoizeResults(false);

        window = SparsePatterns.keySpace(pattern);
        cycleKeys = (1L << logCycleRows) * SparsePatterns.keysPerRow(pattern);
        parent = new QueryTable(SparsePatterns.keys(pattern, 0, window).toTracking(), Map.of("V", new RowKeyValues()));
        parent.setRefreshing(true);
        valuesModified = parent.newModifiedColumnSet("V");
        result = locked(() -> parent.update(FORMULA));
        windowStart = 0;
        modifyStart = 0;
        System.out.println("\nstorage=" + storage + ", pattern=" + pattern + ", threads=" + threads + ", rows="
                + result.size() + ", X=" + result.getColumnSource("X").getClass().getSimpleName()
                + ", estimateSize=" + estimateSize());
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        System.out.println("\nstorage=" + storage + ", pattern=" + pattern + ", threads=" + threads + ", rows="
                + result.size() + ", estimateSize=" + estimateSize());
    }

    private long estimateSize() {
        final ColumnSource<?> column = result.getColumnSource("X");
        return ((SparseArrayColumnSource<?>) column).estimateSize();
    }

    @Benchmark
    public long initialUpdate() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            return locked(() -> parent.update(FORMULA)).size();
        }
    }

    @Benchmark
    public Table marchCycle() {
        final long removeStart = windowStart;
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removed = SparsePatterns.keys(pattern, removeStart, removeStart + cycleKeys);
            final RowSet added = SparsePatterns.keys(pattern, removeStart + window, removeStart + window + cycleKeys);
            final WritableRowSet rowSet = parent.getRowSet().writableCast();
            rowSet.remove(removed);
            rowSet.insert(added);
            parent.notifyListeners(new TableUpdateImpl(added, removed, RowSetFactory.empty(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });
        windowStart += cycleKeys;
        return result;
    }

    @Benchmark
    public Table modifyCycle() {
        final long first = windowStart + modifyStart;
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet modified = SparsePatterns.keys(pattern, first, first + cycleKeys);
            parent.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(), modified,
                    RowSetShiftData.EMPTY, valuesModified));
        });
        modifyStart = (modifyStart + cycleKeys) % window;
        return result;
    }

    private static <T> T locked(final Supplier<T> operation) {
        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(operation::get);
    }

    private static void setQueryTableFlag(final String name, final boolean value) {
        try {
            final Field field = QueryTable.class.getDeclaredField(name);
            field.setAccessible(true);
            field.setBoolean(null, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to set QueryTable." + name, e);
        }
    }

    public static void main(final String[] args) {
        BenchUtil.run(SparseUpdateBenchmark.class);
    }
}
