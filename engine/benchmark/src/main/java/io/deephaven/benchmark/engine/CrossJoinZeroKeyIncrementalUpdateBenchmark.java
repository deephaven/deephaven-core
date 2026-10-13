//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.util.OuterJoinTools;
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
 * Measures the cost of one update cycle of a zero-key cross join (or left outer join) of a small refreshing left table
 * against a large refreshing right table. The right table holds every other row key, so its row set has one range per
 * row. The benchmarks are:
 * <ul>
 * <li>{@link #rightModify()}: one right row is modified in a column the join adds.</li>
 * <li>{@link #rightModifyUnusedColumn()}: one right row is modified in a column the join does not add.</li>
 * <li>{@link #rightAdd()}: a few right rows are added between existing right rows.</li>
 * <li>{@link #rightRemove()}: a few existing right rows are removed.</li>
 * <li>{@link #leftAdd()}: one left row is added after the last left row.</li>
 * </ul>
 * Each add or remove is undone by an unmeasured cycle after the invocation, so every invocation starts from the same
 * tables. All columns compute their values from the row key, so adding, removing and modifying rows costs nothing in
 * the columns themselves and the measured cycle is the join's own work plus the update graph's bookkeeping.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(2)
public class CrossJoinZeroKeyIncrementalUpdateBenchmark {

    /** A long column whose value at a row key is {@code rowKey * multiplier}. */
    private static final class ComputedSource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long multiplier;

        private ComputedSource(final long multiplier) {
            super(long.class);
            this.multiplier = multiplier;
        }

        @Override
        public long getLong(final long rowKey) {
            return rowKey * multiplier;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return rowKey * multiplier;
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

    @Param({"join", "leftOuterJoin"})
    private String joinType;

    @Param({"1000"})
    private int leftSize;

    @Param({"100000"})
    private int rightSize;

    /** The number of right rows added or removed per cycle by {@link #rightAdd()} and {@link #rightRemove()}. */
    @Param({"4"})
    private int changedRightRows;

    private ControlledUpdateGraph updateGraph;

    private QueryTable left;
    private QueryTable right;
    private Table result;

    private ModifiedColumnSet addedColumnModified;
    private ModifiedColumnSet unusedColumnModified;

    private WritableRowSet rightModifiedRows;
    private WritableRowSet rightAddedRows;
    private WritableRowSet rightRemovedRows;
    private WritableRowSet leftAddedRows;

    /**
     * The change that restores the tables after the last invocation, or null when the invocation left them as they
     * were.
     */
    private Runnable undo;

    private SafeCloseable executionContext;

    @Setup(Level.Trial)
    public void setupEnv() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        left = new QueryTable(RowSetFactory.flat(leftSize).toTracking(), Map.of("LS", new ComputedSource(3)));
        left.setRefreshing(true);

        final RowSetBuilderSequential rightBuilder = RowSetFactory.builderSequential();
        for (int ii = 0; ii < rightSize; ++ii) {
            rightBuilder.appendKey(2L * ii);
        }
        final Map<String, ColumnSource<?>> rightColumns = new LinkedHashMap<>();
        rightColumns.put("RS", new ComputedSource(5));
        rightColumns.put("RU", new ComputedSource(7));
        right = new QueryTable(rightBuilder.build().toTracking(), rightColumns);
        right.setRefreshing(true);
        addedColumnModified = right.newModifiedColumnSet("RS");
        unusedColumnModified = right.newModifiedColumnSet("RU");

        result = updateGraph.sharedLock().computeLocked(() -> {
            if (joinType.equals("join")) {
                return left.join(right, "", "RS");
            }
            return OuterJoinTools.leftOuterJoin(left, right, MatchPairFactory.getExpressions(),
                    MatchPairFactory.getExpressions("RS"));
        });

        final long middleKey = 2L * (rightSize / 2);
        rightModifiedRows = RowSetFactory.fromKeys(middleKey);
        final RowSetBuilderSequential addedBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential removedBuilder = RowSetFactory.builderSequential();
        for (int ii = 0; ii < changedRightRows; ++ii) {
            addedBuilder.appendKey(middleKey + 2L * ii + 1);
            removedBuilder.appendKey(middleKey + 2L * ii);
        }
        rightAddedRows = addedBuilder.build();
        rightRemovedRows = removedBuilder.build();
        leftAddedRows = RowSetFactory.fromKeys(leftSize);
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        rightModifiedRows.close();
        rightAddedRows.close();
        rightRemovedRows.close();
        leftAddedRows.close();
        executionContext.close();
    }

    @TearDown(Level.Invocation)
    public void undoInvocation() {
        if (undo != null) {
            updateGraph.runWithinUnitTestCycle(undo::run);
            undo = null;
        }
    }

    private static void addRows(final QueryTable table, final RowSet rows) {
        table.getRowSet().writableCast().insert(rows);
        table.notifyListeners(rows.copy(), RowSetFactory.empty(), RowSetFactory.empty());
    }

    private static void removeRows(final QueryTable table, final RowSet rows) {
        table.getRowSet().writableCast().remove(rows);
        table.notifyListeners(RowSetFactory.empty(), rows.copy(), RowSetFactory.empty());
    }

    private void modifyRight(final ModifiedColumnSet modifiedColumns) {
        updateGraph.runWithinUnitTestCycle(() -> right.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(),
                RowSetFactory.empty(), rightModifiedRows.copy(), RowSetShiftData.EMPTY, modifiedColumns)));
    }

    @Benchmark
    public Table rightModify() {
        modifyRight(addedColumnModified);
        return result;
    }

    @Benchmark
    public Table rightModifyUnusedColumn() {
        modifyRight(unusedColumnModified);
        return result;
    }

    @Benchmark
    public Table rightAdd() {
        updateGraph.runWithinUnitTestCycle(() -> addRows(right, rightAddedRows));
        undo = () -> removeRows(right, rightAddedRows);
        return result;
    }

    @Benchmark
    public Table rightRemove() {
        updateGraph.runWithinUnitTestCycle(() -> removeRows(right, rightRemovedRows));
        undo = () -> addRows(right, rightRemovedRows);
        return result;
    }

    @Benchmark
    public Table leftAdd() {
        updateGraph.runWithinUnitTestCycle(() -> addRows(left, leftAddedRows));
        undo = () -> removeRows(left, leftAddedRows);
        return result;
    }

    public static void main(String[] args) {
        BenchUtil.run(CrossJoinZeroKeyIncrementalUpdateBenchmark.class);
    }
}
