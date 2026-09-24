//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures update cycles of a keyed {@code sumBy} over a refreshing table, with and without reclaiming the states of
 * removed keys ({@code reclaimStates}).
 *
 * <p>
 * Each measured iteration is a batch of {@link #CYCLES} update cycles against a freshly built aggregation, so the
 * reported score is the time for the whole batch; divide by {@link #CYCLES} for the cost of one cycle. Rebuilding for
 * each iteration keeps the table from growing without bound across iterations.
 * </p>
 *
 * <ul>
 * <li>{@link #addOnly()}: each cycle appends {@code rowsPerCycle} rows whose keys have never been seen, and nothing is
 * ever removed. No state is ever emptied, so reclaiming can only add overhead.</li>
 * <li>{@link #slidingWindow()}: the table is a window of {@code windowSize} rows; each cycle removes the oldest
 * {@code rowsPerCycle} rows and appends as many new ones, emptying the oldest keys' states and creating states for the
 * newest keys. With {@code keysReturn == false} every new key is one never seen before, so without reclaiming the hash
 * table and the result's output positions grow with every cycle. With {@code keysReturn == true} the keys cycle through
 * a space twice the size of the window, so each key returns after its state has been emptied; without reclaiming the
 * empty state is simply reused, and with reclaiming it is removed and later re-inserted.</li>
 * </ul>
 *
 * <p>
 * Key and value columns are computed from the row key, so the measured cycle is the aggregation's own work plus the
 * update graph's bookkeeping. Each key owns {@code rowsPerKey} consecutive row keys.
 * </p>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, batchSize = AggregationIncrementalBenchmark.CYCLES)
@Measurement(iterations = 10, batchSize = AggregationIncrementalBenchmark.CYCLES)
@Fork(1)
public class AggregationIncrementalBenchmark {

    /** The number of update cycles in each measured batch. */
    static final int CYCLES = 200;

    @Param({"false", "true"})
    private boolean reclaimStates;

    /** The initial size for {@link #addOnly()}, and the constant size for {@link #slidingWindow()}. */
    @Param({"1000000"})
    private int windowSize;

    @Param({"10000"})
    private int rowsPerCycle;

    @Param({"1", "100"})
    private int rowsPerKey;

    /** Whether {@link #slidingWindow()} keys return after their states are emptied. Ignored by {@link #addOnly()}. */
    @Param({"false", "true"})
    private boolean keysReturn;

    private ControlledUpdateGraph updateGraph;
    private SafeCloseable executionContext;

    private LivenessScope iterationScope;
    private QueryTable addOnlySource;
    private Table addOnlyResult;
    private QueryTable windowSource;
    private Table windowResult;
    /** The first row key of the window, or the next row key to append for {@link #addOnly()}. */
    private long windowFirstKey;
    private long addOnlyNextKey;

    @Setup(Level.Trial)
    public void setupEnv() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
        AggregationStateBenchSupport.setReclaimStates(reclaimStates);
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        iterationScope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(iterationScope, false)) {
            addOnlySource = makeSource(0);
            addOnlyResult = updateGraph.sharedLock().computeLocked(() -> addOnlySource.sumBy("Key"));
            addOnlyNextKey = windowSize;

            final long keySpace = keysReturn ? 2L * (windowSize / rowsPerKey) : 0;
            windowSource = makeSource(keySpace);
            windowResult = updateGraph.sharedLock().computeLocked(() -> windowSource.sumBy("Key"));
            windowFirstKey = 0;
        }
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        // without reclaiming, the output positions keep growing past the number of live states
        System.out.println("slidingWindow result: size=" + windowResult.size() + ", outputPositions="
                + (windowResult.getRowSet().lastRowKey() + 1));
        iterationScope.release();
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        executionContext.close();
    }

    private QueryTable makeSource(final long keySpace) {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put("Key", new AggregationStateBenchSupport.ComputedLongSource(rowsPerKey, keySpace));
        columns.put("Value", new AggregationStateBenchSupport.ComputedLongSource(1, 1000));
        final QueryTable table = new QueryTable(RowSetFactory.flat(windowSize).toTracking(), columns);
        table.setRefreshing(true);
        return table;
    }

    @Benchmark
    public Table addOnly() {
        final long firstAdded = addOnlyNextKey;
        updateGraph.runWithinUnitTestCycle(() -> {
            final WritableRowSet added = RowSetFactory.fromRange(firstAdded, firstAdded + rowsPerCycle - 1);
            addOnlySource.getRowSet().writableCast().insert(added);
            addOnlySource.notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        });
        addOnlyNextKey += rowsPerCycle;
        return addOnlyResult;
    }

    @Benchmark
    public Table slidingWindow() {
        final long firstRemoved = windowFirstKey;
        final long firstAdded = windowFirstKey + windowSize;
        updateGraph.runWithinUnitTestCycle(() -> {
            final WritableRowSet removed = RowSetFactory.fromRange(firstRemoved, firstRemoved + rowsPerCycle - 1);
            final WritableRowSet added = RowSetFactory.fromRange(firstAdded, firstAdded + rowsPerCycle - 1);
            windowSource.getRowSet().writableCast().update(added, removed);
            windowSource.notifyListeners(added, removed, RowSetFactory.empty());
        });
        windowFirstKey += rowsPerCycle;
        return windowResult;
    }

    public static void main(String[] args) {
        BenchUtil.run(AggregationIncrementalBenchmark.class);
    }
}
