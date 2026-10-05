//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
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
 * Measures the cost of one naturalJoin update cycle for two left-side update shapes that the join listeners handle
 * without any hashing of new keys:
 * <ul>
 * <li>{@link #leftShiftBothRefreshing()}: every left row shifts by one row key (as a sorted or prepended upstream
 * produces) while both sides refresh, so the state manager must move every left row within its key's row set.</li>
 * <li>{@link #leftRowsReplacedStaticRight()}: a block of left rows is removed and re-added at the same row keys against
 * a static right table, so the left listener must probe the new rows and store their redirections.</li>
 * </ul>
 * The left table assigns keys round-robin ({@code Key = rowKey % keyCount}), so each key owns a strided set of row keys
 * that the row set implementation stores as a bitmap rather than a range. The left key column computes its value from
 * the row key, so shifting or re-adding left rows costs nothing in the column itself and the measured cycle is the
 * join's own work plus the update graph's bookkeeping.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(2)
public class NaturalJoinIncrementalUpdateBenchmark {

    /**
     * A long key column whose value at a row key is {@code (rowKey - offset) % keyCount}. A shift of the whole table by
     * {@code delta} is applied by adding {@code delta} to the offset, which moves every value along with its row.
     */
    private static final class ComputedKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long keyCount;
        private long offset;
        private long prevOffset;

        private ComputedKeySource(final long keyCount) {
            super(long.class);
            this.keyCount = keyCount;
        }

        void shift(final long delta) {
            prevOffset = offset;
            offset += delta;
        }

        void endCycle() {
            prevOffset = offset;
        }

        @Override
        public long getLong(final long rowKey) {
            return (rowKey - offset) % keyCount;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return (rowKey - prevOffset) % keyCount;
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
    private int leftSize;

    @Param({"1000"})
    private int keyCount;

    /** The number of left rows removed and re-added per cycle by {@link #leftRowsReplacedStaticRight()}. */
    @Param({"100000"})
    private int replacedRows;

    private ControlledUpdateGraph updateGraph;

    private QueryTable shiftLeft;
    private ComputedKeySource shiftLeftKeys;
    private Table shiftResult;
    private long shiftedFirstKey;

    private QueryTable replaceLeft;
    private Table replaceResult;
    private WritableRowSet replacedRowSet;

    @Setup(Level.Trial)
    public void setupEnv() {
        TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        final long[] rightKeys = new long[keyCount];
        final long[] rightValues = new long[keyCount];
        for (int ii = 0; ii < keyCount; ++ii) {
            rightKeys[ii] = ii;
            rightValues[ii] = 1000L + ii;
        }

        shiftLeftKeys = new ComputedKeySource(keyCount);
        shiftLeft = new QueryTable(RowSetFactory.flat(leftSize).toTracking(), Map.of("Key", shiftLeftKeys));
        shiftLeft.setRefreshing(true);
        final QueryTable shiftRight = TstUtils.testRefreshingTable(RowSetFactory.flat(keyCount).toTracking(),
                TableTools.longCol("Key", rightKeys), TableTools.longCol("RS", rightValues));
        shiftResult = updateGraph.sharedLock().computeLocked(() -> shiftLeft.naturalJoin(shiftRight, "Key", "RS"));
        shiftedFirstKey = 0;

        replaceLeft = new QueryTable(RowSetFactory.flat(leftSize).toTracking(),
                Map.of("Key", new ComputedKeySource(keyCount)));
        replaceLeft.setRefreshing(true);
        replaceLeft.setFlat();
        final Table replaceRight = TableTools.newTable(TableTools.longCol("Key", rightKeys),
                TableTools.longCol("RS", rightValues));
        replaceResult =
                updateGraph.sharedLock().computeLocked(() -> replaceLeft.naturalJoin(replaceRight, "Key", "RS"));
        // the last rows of the left table, so that the replaced block ends at the table's last row key
        replacedRowSet = RowSetFactory.fromRange(leftSize - replacedRows, leftSize - 1);
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        replacedRowSet.close();
    }

    @Benchmark
    public Table leftShiftBothRefreshing() {
        final long firstKey = shiftedFirstKey;
        final long lastKey = firstKey + leftSize - 1;
        final long shiftDelta = 1;
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(firstKey, lastKey, shiftDelta);
            final RowSetShiftData shiftData = shiftBuilder.build();
            shiftLeftKeys.shift(shiftDelta);
            shiftData.apply(shiftLeft.getRowSet().writableCast());
            shiftLeft.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(),
                    RowSetFactory.empty(), shiftData, ModifiedColumnSet.EMPTY));
        });
        shiftLeftKeys.endCycle();
        shiftedFirstKey += shiftDelta;
        return shiftResult;
    }

    @Benchmark
    public Table leftRowsReplacedStaticRight() {
        // the key column is computed from the row key, so removing and re-adding the same rows changes no data
        updateGraph.runWithinUnitTestCycle(() -> replaceLeft.notifyListeners(replacedRowSet.copy(),
                replacedRowSet.copy(), RowSetFactory.empty()));
        return replaceResult;
    }

    public static void main(String[] args) {
        BenchUtil.run(NaturalJoinIncrementalUpdateBenchmark.class);
    }
}
