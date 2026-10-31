//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
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
import io.deephaven.engine.table.impl.sources.SparseBlockColumnSource;
import io.deephaven.engine.table.impl.sources.SparseMemoryColumnSourceFactory;
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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;

/**
 * Measures {@code update()} into the sparse array sources and the adaptive sparse array sources, selected by
 * {@link SparseMemoryColumnSourceFactory#USE_ADAPTIVE}, for a refreshing parent whose row keys follow a pattern over a
 * window of {@link #WINDOW} row keys:
 * <ul>
 * <li>Dense: every row key</li>
 * <li>Stride8, Stride64, Stride512: every 8th, 64th or 512th row key</li>
 * <li>Random1: each row key with probability 1%</li>
 * <li>Clustered: an eighth of the leaves full, and up to 4 row keys in each other leaf</li>
 * </ul>
 * The benchmarks are:
 * <ul>
 * <li>{@link #initialUpdate()}: computing the result of the whole parent</li>
 * <li>{@link #marchCycle()}: a cycle that removes the parent's lowest {@link #CYCLE_KEYS} row keys and adds as many
 * beyond its highest, so that the result fills new leaves and releases old ones</li>
 * <li>{@link #modifyCycle()}: a cycle that modifies every parent row within {@link #CYCLE_KEYS} row keys, so that the
 * result records previous values for them</li>
 * </ul>
 * The parent's value column is computed from the row key, so the measured work is the result's. The retained size that
 * {@link SparseBlockColumnSource#estimateSize()} reports for the result column is printed at the end of each trial.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class SparseUpdateBenchmark {
    static final long WINDOW = 1L << 23;
    static final long CYCLE_KEYS = 1L << 16;
    private static final String FORMULA = "X = V * 2.0";

    /**
     * A long column whose value at a row key is {@code rowKey + offset}; incrementing the offset changes every value.
     */
    private static final class ComputedSource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private long offset;
        private long prevOffset;

        private ComputedSource() {
            super(long.class);
        }

        @Override
        public long getLong(final long rowKey) {
            return rowKey + offset;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return rowKey + prevOffset;
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

    @Param({"false", "true"})
    private boolean adaptive;

    @Param({"Dense", "Stride8", "Stride64", "Stride512", "Random1", "Clustered"})
    private String pattern;

    @Param({"serial", "parallel"})
    private String threads;

    private ControlledUpdateGraph updateGraph;
    private QueryTable parent;
    private ComputedSource values;
    private ModifiedColumnSet valuesModified;
    private Table result;
    private long windowStart;
    private long modifyStart;

    @Setup(Level.Trial)
    public void setupTrial() {
        TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        SparseMemoryColumnSourceFactory.USE_ADAPTIVE = adaptive;
        final boolean parallel = threads.equals("parallel");
        setQueryTableFlag("ENABLE_PARALLEL_SELECT_AND_UPDATE", parallel);
        // divide a cycle's rows between several jobs
        QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = 1L << 14;

        values = new ComputedSource();
        parent = new QueryTable(keys(pattern, 0, WINDOW).toTracking(), Map.of("V", values));
        parent.setRefreshing(true);
        valuesModified = parent.newModifiedColumnSet("V");
        result = locked(() -> parent.update(FORMULA));
        windowStart = 0;
        modifyStart = 0;
        System.out.println("\nadaptive=" + adaptive + ", pattern=" + pattern + ", threads=" + threads + ", rows="
                + result.size() + ", X=" + result.getColumnSource("X").getClass().getSimpleName()
                + ", estimateSize=" + estimateSize());
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        System.out.println("\nadaptive=" + adaptive + ", pattern=" + pattern + ", threads=" + threads + ", rows="
                + result.size() + ", estimateSize=" + estimateSize());
    }

    private long estimateSize() {
        final ColumnSource<?> column = result.getColumnSource("X");
        return ((SparseBlockColumnSource) column).estimateSize();
    }

    @Benchmark
    public long initialUpdate() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            return locked(() -> parent.update(FORMULA)).size();
        }
    }

    @Benchmark
    public Table marchCycle() {
        final long removeStart = windowStart;
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removed = keys(pattern, removeStart, removeStart + CYCLE_KEYS);
            final RowSet added = keys(pattern, removeStart + WINDOW, removeStart + WINDOW + CYCLE_KEYS);
            final WritableRowSet rowSet = parent.getRowSet().writableCast();
            rowSet.remove(removed);
            rowSet.insert(added);
            parent.notifyListeners(new TableUpdateImpl(added, removed, RowSetFactory.empty(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });
        windowStart += CYCLE_KEYS;
        return result;
    }

    @Benchmark
    public Table modifyCycle() {
        final long first = windowStart + modifyStart;
        updateGraph.runWithinUnitTestCycle(() -> {
            values.prevOffset = values.offset;
            ++values.offset;
            final RowSet modified = keys(pattern, first, first + CYCLE_KEYS);
            parent.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(), modified,
                    RowSetShiftData.EMPTY, valuesModified));
        });
        values.prevOffset = values.offset;
        modifyStart = (modifyStart + CYCLE_KEYS) % WINDOW;
        return result;
    }

    /**
     * @return the row keys of {@code pattern} from {@code start} (inclusive) to {@code end} (exclusive), both multiples
     *         of the block size
     */
    static WritableRowSet keys(final String pattern, final long start, final long end) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        switch (pattern) {
            case "Dense":
                return RowSetFactory.fromRange(start, end - 1);
            case "Stride8":
                return stride(start, end, 8);
            case "Stride64":
                return stride(start, end, 64);
            case "Stride512":
                return stride(start, end, 512);
            case "Random1":
                for (long key = start; key < end; ++key) {
                    if (Long.remainderUnsigned(mix(key), 100) == 0) {
                        builder.appendKey(key);
                    }
                }
                return builder.build();
            case "Clustered":
                for (long leafStart = start; leafStart < end; leafStart += BLOCK_SIZE) {
                    if (Long.remainderUnsigned(mix(leafStart), 8) == 0) {
                        builder.appendRange(leafStart, leafStart + BLOCK_SIZE - 1);
                    } else {
                        // up to 4 distinct offsets, appended in order
                        final long offsets = mix(leafStart + 1);
                        long previous = -1;
                        final long[] sorted = new long[4];
                        for (int ii = 0; ii < 4; ++ii) {
                            sorted[ii] = (offsets >>> (ii * 10)) & (BLOCK_SIZE - 1);
                        }
                        Arrays.sort(sorted);
                        for (final long offset : sorted) {
                            if (offset != previous) {
                                builder.appendKey(leafStart + offset);
                                previous = offset;
                            }
                        }
                    }
                }
                return builder.build();
            default:
                throw new IllegalArgumentException("Unknown pattern " + pattern);
        }
    }

    private static WritableRowSet stride(final long start, final long end, final int stride) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long key = start; key < end; key += stride) {
            builder.appendKey(key);
        }
        return builder.build();
    }

    private static long mix(final long key) {
        long value = key * 0x9E3779B97F4A7C15L;
        value ^= value >>> 32;
        value *= 0xD6E8FEB86659FD93L;
        return value ^ (value >>> 32);
    }

    private static <T> T locked(final Supplier<T> operation) {
        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(operation::get);
    }

    private static void setQueryTableFlag(final String name, final boolean value) {
        try {
            final Field field = QueryTable.class.getDeclaredField(name);
            field.setAccessible(true);
            field.setBoolean(null, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to set QueryTable." + name, e);
        }
    }

    public static void main(final String[] args) {
        BenchUtil.run(SparseUpdateBenchmark.class);
    }
}
