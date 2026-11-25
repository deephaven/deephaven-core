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
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableConstantDoubleSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.QueryConstants;
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
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

/**
 * Measures a cycle of an as-of join of a static left table of {@code 2^20} rows, every {@code stride}-th row key from
 * 0, whose refreshing right table restamps {@code 2^16} left rows in {@code runs} runs. With one run the join has no
 * keys; otherwise the left rows are dealt round robin to {@code runs} keys. Each key has a right row with stamp 0, and
 * cycles alternate between adding a second right row to every key, which restamps the last sixteenth of its left rows
 * in stamp order, and removing it again.
 * <p>
 * With {@code stamps} "rowKey" a left row's stamp is its row key, so the restamped rows of a key are in row key order;
 * with "scrambled" the stamps are a permutation of the row keys, so the restamped rows are spread over the table.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(2)
public class AsOfJoinRestampBenchmark {
    private static final int LOG_ROWS = 20;
    private static final long ROW_MASK = (1L << LOG_ROWS) - 1;

    @Param({"1", "256", "65536"})
    private int runs;

    @Param({"rowKey", "scrambled"})
    private String stamps;

    @Param({"1", "4"})
    private int stride;

    private ControlledUpdateGraph updateGraph;
    private QueryTable right;
    private Table result;
    private RowSet restampingRows;
    private boolean restampingPresent;

    @Setup(Level.Trial)
    public void setupTrial() {
        TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        final RowSetBuilderSequential leftBuilder = RowSetFactory.builderSequential();
        for (long ii = 0; ii < 1L << LOG_ROWS; ++ii) {
            leftBuilder.appendKey(ii * stride);
        }
        final int leftStride = stride;
        final LongUnaryOperator leftStamp = stamps.equals("rowKey")
                ? rowKey -> rowKey
                : rowKey -> ((rowKey / leftStride * 0x9E3779B1L) & ROW_MASK) * leftStride;
        final long keyCount = runs;
        final Map<String, ColumnSource<?>> leftColumns = new LinkedHashMap<>();
        leftColumns.put("K", new FunctionLongSource(rowKey -> (rowKey / leftStride) % keyCount));
        leftColumns.put("S", new FunctionLongSource(leftStamp));
        final QueryTable left = new QueryTable(leftBuilder.build().toTracking(), leftColumns);

        // right row 2 * key has stamp 0; right row 2 * key + 1, present every other cycle, restamps the left rows of
        // the key whose stamps are in the last sixteenth
        final long restampFrom = (15L << (LOG_ROWS - 4)) * stride;
        final Map<String, ColumnSource<?>> rightColumns = new LinkedHashMap<>();
        rightColumns.put("K", new FunctionLongSource(rowKey -> rowKey / 2));
        rightColumns.put("S", new FunctionLongSource(rowKey -> rowKey % 2 == 0 ? 0 : restampFrom));
        rightColumns.put("R", new ImmutableConstantDoubleSource(1.0));
        final RowSetBuilderSequential rightBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential restampingBuilder = RowSetFactory.builderSequential();
        for (long key = 0; key < keyCount; ++key) {
            rightBuilder.appendKey(2 * key);
            restampingBuilder.appendKey(2 * key + 1);
        }
        right = new QueryTable(rightBuilder.build().toTracking(), rightColumns);
        right.setRefreshing(true);
        restampingRows = restampingBuilder.build();
        restampingPresent = false;
        result = locked(() -> runs == 1 ? left.aj(right, "S", "R") : left.aj(right, "K, S", "R"));
    }

    @Benchmark
    public Table cycle() {
        updateGraph.runWithinUnitTestCycle(() -> {
            final TrackingWritableRowSet rowSet = right.getRowSet().writableCast();
            if (restampingPresent) {
                rowSet.remove(restampingRows);
                right.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), restampingRows.copy(),
                        RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            } else {
                rowSet.insert(restampingRows);
                right.notifyListeners(new TableUpdateImpl(restampingRows.copy(), RowSetFactory.empty(),
                        RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            }
            restampingPresent = !restampingPresent;
        });
        return result;
    }

    /** A long column whose value is a function of the row key. */
    private static final class FunctionLongSource extends AbstractColumnSource<Long>
            implements ImmutableColumnSourceGetDefaults.ForLong {
        private final LongUnaryOperator function;

        private FunctionLongSource(final LongUnaryOperator function) {
            super(Long.class);
            this.function = function;
        }

        @Override
        public long getLong(final long rowKey) {
            return rowKey < 0 ? QueryConstants.NULL_LONG : function.applyAsLong(rowKey);
        }
    }

    private static <T> T locked(final Supplier<T> operation) {
        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(operation::get);
    }

    public static void main(final String[] args) {
        BenchUtil.run(AsOfJoinRestampBenchmark.class);
    }
}
