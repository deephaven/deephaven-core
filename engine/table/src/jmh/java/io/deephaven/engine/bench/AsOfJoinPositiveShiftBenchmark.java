//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.bench;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.jetbrains.annotations.NotNull;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

/**
 * An incremental as-of join whose only change each cycle is a positive shift of every row of one side.
 *
 * <p>
 * Each side holds {@code rows} rows at row keys {@code 2 * ii + offset}. A cycle adds 2 to the offset of the shifted
 * side, so every row moves onto the key its successor vacates. Column values follow their rows: row {@code ii} has
 * stamp {@code 2 * ii} on the right and {@code 2 * ii + 1} on the left, and bucket {@code ii % buckets}; right row
 * {@code ii} also has sentinel {@code ii}. Every left row therefore matches a right row, and a right shift restamps the
 * redirection of every left row.
 * </p>
 *
 * <p>
 * The {@link Path} parameter selects the join implementation and the shifted side:
 * </p>
 * <ul>
 * <li>{@code ZERO_KEY_STATIC_LEFT}: zero-key join of a static left table; the right side shifts.</li>
 * <li>{@code ZERO_KEY_RIGHT} and {@code ZERO_KEY_LEFT}: zero-key join of two refreshing tables.</li>
 * <li>{@code BUCKETED_RIGHT} and {@code BUCKETED_LEFT}: bucketed join of two refreshing tables.</li>
 * </ul>
 *
 * <pre>
 * ./gradlew engine-table:jmhJar
 * java -jar engine/table/build/libs/deephaven-engine-table-&lt;version&gt;-jmh.jar AsOfJoinPositiveShiftBenchmark -prof gc
 * </pre>
 */
@Fork(value = 2, jvmArgs = {"-Xms8G", "-Xmx8G"})
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
public class AsOfJoinPositiveShiftBenchmark {
    static {
        System.setProperty("Configuration.rootFile", "dh-tests.prop");
        System.setProperty("workspace", "build/workspace");
    }

    public enum Path {
        ZERO_KEY_STATIC_LEFT, ZERO_KEY_RIGHT, ZERO_KEY_LEFT, BUCKETED_RIGHT, BUCKETED_LEFT
    }

    @Param({"ZERO_KEY_STATIC_LEFT", "ZERO_KEY_RIGHT", "ZERO_KEY_LEFT", "BUCKETED_RIGHT", "BUCKETED_LEFT"})
    public Path path;

    @Param({"1000000"})
    public int rows;

    @Param({"4"})
    public int buckets;

    private EngineCleanup engine;
    private ControlledUpdateGraph updateGraph;
    private LivenessScope scope;

    private ShiftingSide shiftedSide;
    private Table result;
    private BlackholeListener listener;

    /**
     * The row keys of one side and the offset that maps a row key back to its row, with the offset of the previous
     * cycle for previous values.
     */
    private static class ShiftingSide {
        private final TrackingWritableRowSet rowSet;
        private final QueryTable table;
        private long offset;
        private long prevOffset;

        private ShiftingSide(final int rows, final boolean refreshing, final int buckets, final long stampAddend) {
            rowSet = RowSetFactory.empty().toTracking();
            for (long ii = 0; ii < rows; ++ii) {
                rowSet.insert(2 * ii);
            }
            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            columns.put("Bucket", new RowIndexSource(this, index -> index % buckets));
            columns.put("Stamp", new RowIndexSource(this, index -> 2 * index + stampAddend));
            if (stampAddend == 0) {
                columns.put("Sentinel", new RowIndexSource(this, index -> index));
            }
            table = new QueryTable(rowSet, columns);
            table.setRefreshing(refreshing);
        }

        /**
         * Moves every row up by 2 row keys; must be called within an update cycle.
         */
        private void shift() {
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(rowSet.firstRowKey(), rowSet.lastRowKey(), 2);
            final RowSetShiftData shifted = shiftBuilder.build();
            prevOffset = offset;
            offset += 2;
            shifted.apply(rowSet);
            table.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(),
                    RowSetFactory.empty(), shifted, ModifiedColumnSet.EMPTY));
        }
    }

    /**
     * A column whose value is a function of the row a row key holds, which follows the row as the side shifts.
     */
    private static class RowIndexSource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final ShiftingSide side;
        private final LongUnaryOperator valueOfIndex;

        private RowIndexSource(final ShiftingSide side, final LongUnaryOperator valueOfIndex) {
            super(long.class);
            this.side = side;
            this.valueOfIndex = valueOfIndex;
        }

        @Override
        public long getLong(final long rowKey) {
            return valueOfIndex.applyAsLong((rowKey - side.offset) >> 1);
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return valueOfIndex.applyAsLong((rowKey - side.prevOffset) >> 1);
        }

        @Override
        public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> dest,
                @NotNull final RowSequence rowSequence) {
            fill(dest, rowSequence, side.offset);
        }

        @Override
        public void fillPrevChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> dest, @NotNull final RowSequence rowSequence) {
            fill(dest, rowSequence, side.prevOffset);
        }

        private void fill(final WritableChunk<? super Values> dest, final RowSequence rowSequence,
                final long rowOffset) {
            final WritableLongChunk<? super Values> longDest = dest.asWritableLongChunk();
            longDest.setSize(0);
            rowSequence.forAllRowKeys(rowKey -> longDest.add(valueOfIndex.applyAsLong((rowKey - rowOffset) >> 1)));
        }
    }

    @Setup(Level.Trial)
    public void setupTrial(final Blackhole blackhole) throws Exception {
        engine = new EngineCleanup();
        engine.setUp();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final boolean leftRefreshing = path != Path.ZERO_KEY_STATIC_LEFT;
        final boolean keyed = path == Path.BUCKETED_RIGHT || path == Path.BUCKETED_LEFT;
        final boolean shiftLeft = path == Path.ZERO_KEY_LEFT || path == Path.BUCKETED_LEFT;

        scope = new LivenessScope();
        LivenessScopeStack.push(scope);
        updateGraph.startCycleForUnitTests();
        try {
            final ShiftingSide left = new ShiftingSide(rows, leftRefreshing, buckets, 1);
            final ShiftingSide right = new ShiftingSide(rows, true, buckets, 0);
            shiftedSide = shiftLeft ? left : right;
            result = left.table.aj(right.table, (keyed ? "Bucket," : "") + "Stamp>=Stamp", "RightStamp=Stamp,Sentinel");
            listener = new BlackholeListener(blackhole);
            result.addUpdateListener(listener);
        } finally {
            updateGraph.completeCycleForUnitTests();
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() throws Exception {
        result.removeUpdateListener(listener);
        listener = null;
        result = null;
        shiftedSide = null;
        LivenessScopeStack.pop(scope);
        scope.release();
        scope = null;
        updateGraph = null;
        engine.tearDown();
        engine = null;
    }

    @Benchmark
    public void shiftCycle() throws Throwable {
        updateGraph.runWithinUnitTestCycle(shiftedSide::shift);
        if (listener.e != null) {
            throw listener.e;
        }
    }
}
