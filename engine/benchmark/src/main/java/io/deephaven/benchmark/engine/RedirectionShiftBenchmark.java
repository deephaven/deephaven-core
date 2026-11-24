//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableConstantDoubleSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableConstantLongSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Measures a cycle of an operation that keeps a sparse row redirection for a refreshing table of {@code 2^20} rows,
 * every {@code stride}-th row key from 0, dense enough that the operation chooses a sparse row redirection. The
 * operations are:
 * <ul>
 * <li>naturalJoin, aj, ajMerged: a join on a constant key (and for aj, a constant stamp) with a static right table, or
 * for ajMerged a refreshing one, so that the join's own state changes little</li>
 * <li>updateBy: a redirected {@code update_by} of a rolling sum over two rows</li>
 * </ul>
 * With {@code change} "shift", cycles alternate between inserting a row before the last {@code 2^16} rows, shifting
 * them up, and removing it again, shifting them back, so that every cycle moves the redirections of {@code 2^16} rows
 * and the table keeps its size. With "march", a cycle removes the first {@code 2^16} rows and adds as many after the
 * last.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(2)
public class RedirectionShiftBenchmark {
    private static final int LOG_ROWS = 20;
    private static final int LOG_CHANGED = 16;

    @Param({"naturalJoin", "aj", "ajMerged", "updateBy"})
    private String operation;

    @Param({"shift", "march"})
    private String change;

    /**
     * The distance between the table's row keys: 1 or 4, so that each block of 1024 row keys holds 1024 or 256 rows.
     */
    @Param({"1", "4"})
    private int stride;

    private ControlledUpdateGraph updateGraph;
    private QueryTable left;
    private Table result;
    /** The row key of the row a shift cycle inserted, which the next removes, or -1 when no row is inserted. */
    private long insertedKey;
    private long marchStart;
    private long marchEnd;

    @Setup(Level.Trial)
    public void setupTrial() {
        TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long ii = 0; ii < 1L << LOG_ROWS; ++ii) {
            builder.appendKey(ii * stride);
        }
        final WritableRowSet rows = builder.build();
        insertedKey = -1;
        marchStart = 0;
        marchEnd = rows.lastRowKey() + stride;

        final Map<String, ColumnSource<?>> leftColumns = new LinkedHashMap<>();
        leftColumns.put("K", new ImmutableConstantLongSource(0));
        leftColumns.put("S", new ImmutableConstantLongSource(0));
        leftColumns.put("V", new ImmutableConstantDoubleSource(1.0));
        left = new QueryTable(rows.toTracking(), leftColumns);
        left.setRefreshing(true);

        if (operation.equals("updateBy")) {
            result = locked(() -> left.updateBy(UpdateByControl.builder().useRedirection(true).build(),
                    UpdateByOperation.RollingSum(2, "X=V")));
            return;
        }
        final Map<String, ColumnSource<?>> rightColumns = new LinkedHashMap<>();
        rightColumns.put("K", new ImmutableConstantLongSource(0));
        rightColumns.put("S", new ImmutableConstantLongSource(0));
        rightColumns.put("R", new ImmutableConstantDoubleSource(1.0));
        final QueryTable right = new QueryTable(RowSetFactory.flat(1).toTracking(), rightColumns);
        right.setRefreshing(operation.equals("ajMerged"));
        result = locked(() -> operation.equals("naturalJoin")
                ? left.naturalJoin(right, "K", "R")
                : left.aj(right, "K, S", "R"));
    }

    @Benchmark
    public Table cycle() {
        updateGraph.runWithinUnitTestCycle(change.equals("shift") ? this::shift : this::march);
        return result;
    }

    private void shift() {
        final TrackingWritableRowSet rowSet = left.getRowSet().writableCast();
        final long last = rowSet.lastRowKey();
        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
        if (insertedKey < 0) {
            final long firstShifted = rowSet.get(rowSet.size() - (1L << LOG_CHANGED));
            shiftBuilder.shiftRange(firstShifted, last, stride);
            RowSetShiftData.applyShift(rowSet, firstShifted, last, stride);
            final RowSet added = RowSetFactory.fromKeys(firstShifted);
            rowSet.insert(added);
            insertedKey = firstShifted;
            left.notifyListeners(new TableUpdateImpl(added, RowSetFactory.empty(), RowSetFactory.empty(),
                    shiftBuilder.build(), ModifiedColumnSet.EMPTY));
        } else {
            final RowSet removed = RowSetFactory.fromKeys(insertedKey);
            rowSet.remove(removed);
            shiftBuilder.shiftRange(insertedKey + stride, last, -stride);
            RowSetShiftData.applyShift(rowSet, insertedKey + stride, last, -stride);
            insertedKey = -1;
            left.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), removed, RowSetFactory.empty(),
                    shiftBuilder.build(), ModifiedColumnSet.EMPTY));
        }
    }

    private void march() {
        final TrackingWritableRowSet rowSet = left.getRowSet().writableCast();
        final long count = 1L << LOG_CHANGED;
        final RowSetBuilderSequential removedBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential addedBuilder = RowSetFactory.builderSequential();
        for (long ii = 0; ii < count; ++ii) {
            removedBuilder.appendKey(marchStart + ii * stride);
            addedBuilder.appendKey(marchEnd + ii * stride);
        }
        marchStart += count * stride;
        marchEnd += count * stride;
        final RowSet removed = removedBuilder.build();
        final RowSet added = addedBuilder.build();
        rowSet.remove(removed);
        rowSet.insert(added);
        left.notifyListeners(new TableUpdateImpl(added, removed, RowSetFactory.empty(), RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY));
    }

    private static <T> T locked(final Supplier<T> operation) {
        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(operation::get);
    }

    public static void main(final String[] args) {
        BenchUtil.run(RedirectionShiftBenchmark.class);
    }
}
