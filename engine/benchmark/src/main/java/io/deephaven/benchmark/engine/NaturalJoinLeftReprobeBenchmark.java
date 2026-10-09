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
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
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
 * Measures one update cycle of a natural join whose refreshing left table probes a static right table: each cycle
 * removes the last {@code replacedRows} left rows and adds them back at the same row keys, so the left listener probes
 * the key of every added row and stores its redirection.
 * <p>
 * The right table has one row for each of {@code keyCount} keys, in a scrambled order, so the order in which the right
 * keys are built is unrelated to the order in which the left rows probe them. The left key of each row is its row key
 * modulo {@code keyCount}, computed from the row key, so re-adding rows costs nothing in the column itself and the
 * measured cycle is the join's own work plus the update graph's bookkeeping.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(2)
public class NaturalJoinLeftReprobeBenchmark {

    /**
     * A long key column whose value at a row key is the row key modulo {@code keyCount}.
     */
    private static final class ModuloKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long keyCount;

        private ModuloKeySource(final long keyCount) {
            super(long.class);
            this.keyCount = keyCount;
        }

        @Override
        public long getLong(final long rowKey) {
            return rowKey % keyCount;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return getLong(rowKey);
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

    // a prime that divides no power of ten, so that the right keys are a permutation of [0, keyCount)
    private static final long RIGHT_MULTIPLIER = 999_983L;

    @Param({"1000000"})
    private int leftSize;

    @Param({"100000", "1000000", "10000000"})
    private int keyCount;

    /** The number of left rows removed and re-added per cycle. */
    @Param({"100000"})
    private int replacedRows;

    private ControlledUpdateGraph updateGraph;
    private QueryTable left;
    private Table result;
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
            rightKeys[ii] = Math.floorMod(ii * RIGHT_MULTIPLIER, keyCount);
            rightValues[ii] = 1000L + ii;
        }
        final Table right = TableTools.newTable(TableTools.longCol("Key", rightKeys),
                TableTools.longCol("RS", rightValues));

        left = new QueryTable(RowSetFactory.flat(leftSize).toTracking(), Map.of("Key", new ModuloKeySource(keyCount)));
        left.setRefreshing(true);
        left.setFlat();
        result = updateGraph.sharedLock().computeLocked(() -> left.naturalJoin(right, "Key", "RS"));
        // the last rows of the left table, so that the replaced block ends at the table's last row key
        replacedRowSet = RowSetFactory.fromRange(leftSize - replacedRows, leftSize - 1);
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        replacedRowSet.close();
    }

    @Benchmark
    public Table leftRowsReplaced() {
        // the key column is computed from the row key, so removing and re-adding the same rows changes no data
        updateGraph.runWithinUnitTestCycle(
                () -> left.notifyListeners(replacedRowSet.copy(), replacedRowSet.copy(), RowSetFactory.empty()));
        return result;
    }

    public static void main(String[] args) {
        BenchUtil.run(NaturalJoinLeftReprobeBenchmark.class);
    }
}
