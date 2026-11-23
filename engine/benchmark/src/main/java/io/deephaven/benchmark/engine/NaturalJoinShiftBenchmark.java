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
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableConstantLongSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Measures a cycle of a {@code naturalJoin} whose refreshing left table inserts a row before its last
 * {@code 2^logShiftRows} rows, shifting them, so that the join moves the row redirections of the shifted rows. The left
 * table holds {@code 2^20} rows, every {@code stride}-th row key from 0, dense enough that the join chooses a sparse
 * row redirection. The join key is a constant, so that the join's own state changes little and the measured work is the
 * redirection's.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(2)
public class NaturalJoinShiftBenchmark {
    private static final int LOG_ROWS = 20;

    /**
     * The distance between the left table's row keys: 1, 2 or 4, so that each block of 1024 row keys holds 1024, 512 or
     * 256 rows.
     */
    @Param({"1", "2", "4"})
    private int stride;

    @Param({"12", "16"})
    private int logShiftRows;

    private ControlledUpdateGraph updateGraph;
    private QueryTable left;
    private Table result;

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
        left = new QueryTable(builder.build().toTracking(), Map.of("K", new ImmutableConstantLongSource(0)));
        left.setRefreshing(true);
        final Table right = TableTools.newTable(TableTools.longCol("K", 0), TableTools.doubleCol("R", 1.0));
        result = locked(() -> left.naturalJoin(right, "K", "R"));

        final RowRedirection redirection =
                ((RedirectedColumnSource<?>) result.getColumnSource("R")).getRowRedirection();
        System.out.println("\nstride=" + stride + ", logShiftRows=" + logShiftRows
                + ", redirection=" + redirection.getClass().getSimpleName());
    }

    @Benchmark
    public Table shiftCycle() {
        updateGraph.runWithinUnitTestCycle(() -> {
            final TrackingWritableRowSet rowSet = left.getRowSet().writableCast();
            final long last = rowSet.lastRowKey();
            final long firstShifted = rowSet.get(rowSet.size() - (1L << logShiftRows));
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(firstShifted, last, stride);
            final RowSetShiftData shifted = shiftBuilder.build();
            RowSetShiftData.applyShift(rowSet, firstShifted, last, stride);
            final RowSet added = RowSetFactory.fromKeys(firstShifted);
            rowSet.insert(added);
            left.notifyListeners(new TableUpdateImpl(added, RowSetFactory.empty(), RowSetFactory.empty(), shifted,
                    ModifiedColumnSet.EMPTY));
        });
        return result;
    }

    private static <T> T locked(final Supplier<T> operation) {
        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(operation::get);
    }

    public static void main(final String[] args) {
        BenchUtil.run(NaturalJoinShiftBenchmark.class);
    }
}
