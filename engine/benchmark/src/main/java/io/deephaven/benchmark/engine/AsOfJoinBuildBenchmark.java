//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures the initial build of an as-of join of a left table with {@code leftRows} rows onto a right table with
 * {@code rightRows} rows, matching keys drawn from {@code keyCount} values, for each combination of the inputs
 * refreshing:
 * <ul>
 * <li>{@link #staticBuild()}: both static.</li>
 * <li>{@link #leftRefreshingBuild}: the left refreshes, so the join keeps the right rows of each key to restamp left
 * updates.</li>
 * <li>{@link #rightRefreshingBuild}: the right refreshes, so the join keeps the left rows of each key.</li>
 * <li>{@link #bothRefreshingBuild}: both refresh, so the join keeps the rows of every key from either side.</li>
 * </ul>
 * Each side's keys are a scrambled function of the row key modulo {@code keyCount}, scrambled differently on the two
 * sides, so each key's rows are scattered and the order in which a side's keys are first seen is unrelated to the order
 * in which the other side looks them up. With a million rows a side, every key occurs on both sides at 100K keys; at 1M
 * the right holds every key and the left about 88% of them; at 10M each side holds a million distinct keys, of which
 * about a tenth occur on the other side. Each row's stamp is its row key, so the rows of a key are in stamp order. The
 * columns compute their values, so the measured time is the join's own work rather than reading column data.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class AsOfJoinBuildBenchmark {

    /**
     * A long key column whose value at a row key is {@code (rowKey * multiplier) mod keyCount}.
     */
    static final class ScaledKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long multiplier;
        private final long keyCount;

        ScaledKeySource(final long multiplier, final long keyCount) {
            super(long.class);
            this.multiplier = multiplier;
            this.keyCount = keyCount;
        }

        @Override
        public long getLong(final long rowKey) {
            return Math.floorMod(rowKey * multiplier, keyCount);
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return getLong(rowKey);
        }

        @Override
        public boolean isImmutable() {
            return true;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    }

    /**
     * A long stamp column whose value at a row key is the row key.
     */
    static final class RowKeyStampSource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {

        RowKeyStampSource() {
            super(long.class);
        }

        @Override
        public long getLong(final long rowKey) {
            return rowKey;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return rowKey;
        }

        @Override
        public boolean isImmutable() {
            return true;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    }

    // scatters the rows of each key across the left table
    static final long LEFT_MULTIPLIER = 0x9E3779B97F4A7C15L;
    // a prime that divides no power of ten, so that a right table of keyCount rows holds every key once
    static final long RIGHT_MULTIPLIER = 999_983L;

    @Param({"1000000"})
    private int leftRows;

    @Param({"1000000"})
    private int rightRows;

    @Param({"100000", "1000000", "10000000"})
    private int keyCount;

    private ControlledUpdateGraph updateGraph;
    private SafeCloseable executionContext;
    private QueryTable staticLeft;
    private QueryTable staticRight;
    private QueryTable refreshingLeft;
    private QueryTable refreshingRight;

    @Setup(Level.Trial)
    public void setupEnv() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        staticLeft = sideTable("L", leftRows, LEFT_MULTIPLIER, keyCount, false);
        staticRight = sideTable("R", rightRows, RIGHT_MULTIPLIER, keyCount, false);
        refreshingLeft = sideTable("L", leftRows, LEFT_MULTIPLIER, keyCount, true);
        refreshingRight = sideTable("R", rightRows, RIGHT_MULTIPLIER, keyCount, true);
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        executionContext.close();
    }

    /**
     * @return a flat table of {@code size} rows with a key column {@code <prefix>K} and a stamp column
     *         {@code <prefix>S}
     */
    static QueryTable sideTable(final String prefix, final int size, final long multiplier, final long keyCount,
            final boolean refreshing) {
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(), Map.of(
                prefix + "K", new ScaledKeySource(multiplier, keyCount),
                prefix + "S", new RowKeyStampSource()));
        table.setRefreshing(refreshing);
        if (refreshing) {
            table.setFlat();
        }
        return table;
    }

    @Benchmark
    public long staticBuild() {
        return staticLeft.aj(staticRight, "LK=RK,LS>=RS", "RS").size();
    }

    @Benchmark
    public void leftRefreshingBuild(final Blackhole blackhole) {
        build(refreshingLeft, staticRight, blackhole);
    }

    @Benchmark
    public void rightRefreshingBuild(final Blackhole blackhole) {
        build(staticLeft, refreshingRight, blackhole);
    }

    @Benchmark
    public void bothRefreshingBuild(final Blackhole blackhole) {
        build(refreshingLeft, refreshingRight, blackhole);
    }

    private void build(final QueryTable left, final QueryTable right, final Blackhole blackhole) {
        // release the join, and the listeners it registers, after each build
        final LivenessScope scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, true)) {
            final Table result =
                    updateGraph.sharedLock().computeLocked(() -> left.aj(right, "LK=RK,LS>=RS", "RS"));
            blackhole.consume(result.size());
        }
    }

    public static void main(String[] args) {
        BenchUtil.run(AsOfJoinBuildBenchmark.class);
    }
}
