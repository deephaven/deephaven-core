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
 * Measures a cycle of an {@code update_by} without a row redirection, whose outputs are sparse array sources, over a
 * refreshing table of {@code 2^20} rows, every {@code stride}-th row key from 0. Cycles alternate between inserting a
 * row before the last {@code 2^16} rows, shifting them up, and removing it again, shifting them back, so that every
 * cycle moves the output values of {@code 2^16} rows.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(2)
public class UpdateByShiftBenchmark {
    private static final int LOG_ROWS = 20;
    private static final int LOG_SHIFTED = 16;

    @Param({"RollingSum", "CumSum"})
    private String operation;

    @Param({"1", "4"})
    private int stride;

    private ControlledUpdateGraph updateGraph;
    private QueryTable source;
    private Table result;
    /** The row key of the row a cycle inserted, which the next removes, or -1 when no row is inserted. */
    private long insertedKey;

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
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put("V", new ImmutableConstantDoubleSource(1.0));
        source = new QueryTable(builder.build().toTracking(), columns);
        source.setRefreshing(true);
        insertedKey = -1;

        final UpdateByOperation updateByOperation = operation.equals("RollingSum")
                ? UpdateByOperation.RollingSum(2, "X=V")
                : UpdateByOperation.CumSum("X=V");
        result = locked(() -> source.updateBy(UpdateByControl.builder().useRedirection(false).build(),
                updateByOperation));
    }

    @Benchmark
    public Table cycle() {
        updateGraph.runWithinUnitTestCycle(() -> {
            final TrackingWritableRowSet rowSet = source.getRowSet().writableCast();
            final long last = rowSet.lastRowKey();
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            if (insertedKey < 0) {
                final long firstShifted = rowSet.get(rowSet.size() - (1L << LOG_SHIFTED));
                shiftBuilder.shiftRange(firstShifted, last, stride);
                shiftRows(rowSet, firstShifted, last, stride);
                final RowSet added = RowSetFactory.fromKeys(firstShifted);
                rowSet.insert(added);
                insertedKey = firstShifted;
                source.notifyListeners(new TableUpdateImpl(added, RowSetFactory.empty(), RowSetFactory.empty(),
                        shiftBuilder.build(), ModifiedColumnSet.EMPTY));
            } else {
                final RowSet removed = RowSetFactory.fromKeys(insertedKey);
                rowSet.remove(removed);
                shiftBuilder.shiftRange(insertedKey + stride, last, -stride);
                shiftRows(rowSet, insertedKey + stride, last, -stride);
                insertedKey = -1;
                source.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), removed, RowSetFactory.empty(),
                        shiftBuilder.build(), ModifiedColumnSet.EMPTY));
            }
        });
        return result;
    }

    /**
     * Shift the rows of {@code rowSet} in {@code [first, last]} by {@code delta}, rebuilding only those rows rather
     * than the whole row set.
     */
    private static void shiftRows(final TrackingWritableRowSet rowSet, final long first, final long last,
            final long delta) {
        try (final WritableRowSet moving = rowSet.subSetByKeyRange(first, last)) {
            rowSet.removeRange(first, last);
            moving.shiftInPlace(delta);
            rowSet.insert(moving);
        }
    }

    private static <T> T locked(final Supplier<T> operation) {
        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(operation::get);
    }

    public static void main(final String[] args) {
        BenchUtil.run(UpdateByShiftBenchmark.class);
    }
}
