//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
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
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures update cycles of a keyed {@code sumBy} over a refreshing table under each way of reclaiming the states of
 * removed keys ({@code reclaim}): keeping every state, compacting the result, or releasing whole blocks of empty
 * states, optionally collapsing runs of sparse blocks ({@code collapse}).
 *
 * <p>
 * Each measured iteration is a batch of {@link #CYCLES} update cycles against a freshly built aggregation, so the
 * reported score is the time for the whole batch; divide by {@link #CYCLES} for the cost of one cycle. Rebuilding for
 * each iteration keeps the table from growing without bound across iterations. With the default parameters, an
 * iteration that never reuses a key sees 10 million distinct keys.
 * </p>
 *
 * <ul>
 * <li>{@link #addOnly()}: each cycle appends {@code rowsPerCycle} rows whose keys have never been seen, and nothing is
 * ever removed. No state is ever emptied, so reclaiming can only add overhead.</li>
 * <li>{@link #slidingWindow()}: the table is a window of {@code windowSize} rows; each cycle removes the oldest
 * {@code rowsPerCycle} rows and appends as many new ones, so keys die in the order they were created. With
 * {@code keysReturn == false} every new key is one never seen before. With {@code keysReturn == true} the keys cycle
 * through a space twice the size of the window, so each key returns some time after its state has been removed.</li>
 * <li>{@link #randomChurn()}: the table holds {@code windowSize} rows; each cycle removes {@code rowsPerCycle} rows
 * chosen at random and appends as many new ones, so keys die in no particular order and blocks of output positions thin
 * out unevenly. The removals are chosen before the iteration, so the measured cycle does not include them.</li>
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
    static final int CYCLES = 900;

    @Param({"none", "compact", "blocks"})
    private String reclaim;

    /** The fraction free at which blocks are collapsed when releasing blocks; 1 disables collapsing. */
    @Param({"1"})
    private double collapse;

    /**
     * The fraction of the output positions that released blocks at the start must reach before every state is shifted
     * down to reuse them; 0 shifts for any released block, and a negative fraction never shifts.
     */
    @Param({"-1"})
    private double frontShift;

    /** The initial size for {@link #addOnly()}, and the constant size for the other benchmarks. */
    @Param({"1000000"})
    private int windowSize;

    @Param({"10000"})
    private int rowsPerCycle;

    @Param({"1", "100"})
    private int rowsPerKey;

    /** Whether keys return after their states are removed. Ignored by {@link #addOnly()}. */
    @Param({"false", "true"})
    private boolean keysReturn;

    private ControlledUpdateGraph updateGraph;
    private SafeCloseable executionContext;

    /** The rows removed by each cycle of {@link #randomChurn()}, the same for every iteration. */
    private RowSet[] randomRemovals;

    private LivenessScope iterationScope;
    private QueryTable source;
    private Table result;
    /** The next row key to append. */
    private long nextRowKey;
    /** The first row key of the window, for {@link #slidingWindow()}. */
    private long windowFirstKey;
    private int cycle;
    private long maxPosition;
    private long rehashesAtStart;

    @Setup(Level.Trial)
    public void setupEnv(final BenchmarkParams params) {
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
        AggregationStateBenchSupport.setReclaimMode(reclaim);
        AggregationStateBenchSupport.setCollapseFreeFraction(collapse);
        AggregationStateBenchSupport.setFrontShiftFraction(frontShift);
        if (params.getBenchmark().endsWith(".randomChurn")) {
            randomRemovals = chooseRandomRemovals();
        }
    }

    private RowSet[] chooseRandomRemovals() {
        final Random random = new Random(0);
        final long[] live = new long[windowSize];
        for (int ii = 0; ii < windowSize; ++ii) {
            live[ii] = ii;
        }
        long next = windowSize;
        final RowSet[] removals = new RowSet[CYCLES];
        final long[] removed = new long[rowsPerCycle];
        for (int ci = 0; ci < CYCLES; ++ci) {
            final long firstAdded = next;
            int count = 0;
            while (count < rowsPerCycle) {
                // replace a random row that was live at the start of the cycle with one of this cycle's new rows
                final int index = random.nextInt(windowSize);
                if (live[index] >= firstAdded) {
                    continue;
                }
                removed[count++] = live[index];
                live[index] = next++;
            }
            Arrays.sort(removed);
            removals[ci] = RowSetFactory.fromKeys(removed);
        }
        return removals;
    }

    @Setup(Level.Iteration)
    public void setupIteration(final BenchmarkParams params) {
        final boolean isAddOnly = params.getBenchmark().endsWith(".addOnly");
        iterationScope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(iterationScope, false)) {
            final long keySpace = isAddOnly || !keysReturn ? 0 : 2L * (windowSize / rowsPerKey);
            source = makeSource(keySpace);
            result = updateGraph.sharedLock().computeLocked(() -> source.sumBy("Key"));
        }
        nextRowKey = windowSize;
        windowFirstKey = 0;
        cycle = 0;
        maxPosition = result.getRowSet().lastRowKey();
        rehashesAtStart = AggregationStateBenchSupport.rehashCount();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        System.out.println("result: liveStates=" + result.size() + ", positionsAssigned=" + (maxPosition + 1)
                + ", lastLivePosition=" + result.getRowSet().lastRowKey()
                + ", rehashes=" + (AggregationStateBenchSupport.rehashCount() - rehashesAtStart)
                + ", retainedHeapMB=" + retainedHeapMegabytes());
        iterationScope.release();
        source = null;
        result = null;
    }

    private static long retainedHeapMegabytes() {
        System.gc();
        final Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) >> 20;
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        if (randomRemovals != null) {
            for (final RowSet removal : randomRemovals) {
                removal.close();
            }
        }
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

    private void runCycle(final RowSet removed) {
        final long firstAdded = nextRowKey;
        updateGraph.runWithinUnitTestCycle(() -> {
            final WritableRowSet added = RowSetFactory.fromRange(firstAdded, firstAdded + rowsPerCycle - 1);
            source.getRowSet().writableCast().update(added, removed);
            source.notifyListeners(added, removed.copy(), RowSetFactory.empty());
        });
        nextRowKey += rowsPerCycle;
        maxPosition = Math.max(maxPosition, result.getRowSet().lastRowKey());
    }

    @Benchmark
    public Table addOnly() {
        try (final RowSet removed = RowSetFactory.empty()) {
            runCycle(removed);
        }
        return result;
    }

    @Benchmark
    public Table slidingWindow() {
        try (final RowSet removed = RowSetFactory.fromRange(windowFirstKey, windowFirstKey + rowsPerCycle - 1)) {
            runCycle(removed);
        }
        windowFirstKey += rowsPerCycle;
        return result;
    }

    @Benchmark
    public Table randomChurn() {
        runCycle(randomRemovals[cycle++]);
        return result;
    }

    public static void main(String[] args) {
        BenchUtil.run(AggregationIncrementalBenchmark.class);
    }
}
