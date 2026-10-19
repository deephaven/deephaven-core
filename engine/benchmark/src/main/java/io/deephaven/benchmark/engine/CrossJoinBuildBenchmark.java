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
 * Measures the initial build of a keyed cross join ({@code join}) of two tables with {@code rows} rows each, whose keys
 * are drawn from {@code keyCount} values, so that the result has about {@code rows * rows / keyCount} rows:
 * <ul>
 * <li>{@link #staticBuild()}: both inputs static.</li>
 * <li>{@link #leftRefreshingBuild()}: a refreshing left input and a static right input, so the join builds the state it
 * keeps for left updates.</li>
 * <li>{@link #bothRefreshingBuild()}: both inputs refreshing, so the join builds the state it keeps for updates.</li>
 * </ul>
 * The key columns compute a long key from the row key, scattering each key's rows across the table, so the measured
 * time is the join's own work rather than reading column data. The mapping is not one to one: when {@code rows} equals
 * {@code keyCount}, about 88% of the values occur (875,539 of 1,000,000).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class CrossJoinBuildBenchmark {

    /**
     * A long key column whose value at a row key is a scrambled function of the row key modulo {@code keyCount}.
     */
    static final class ScrambledKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long keyCount;

        ScrambledKeySource(final long keyCount) {
            super(long.class);
            this.keyCount = keyCount;
        }

        @Override
        public long getLong(final long rowKey) {
            return Math.floorMod(rowKey * 0x9E3779B97F4A7C15L, keyCount);
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

    @Param({"1000000"})
    private int rows;

    @Param({"1000000", "10000"})
    private int keyCount;

    private ControlledUpdateGraph updateGraph;
    private QueryTable staticLeft;
    private QueryTable staticRight;
    private QueryTable refreshingLeft;
    private QueryTable refreshingRight;
    private SafeCloseable executionContext;

    @Setup(Level.Trial)
    public void setupEnv() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        staticLeft = keyTable("LK", false);
        staticRight = keyTable("RK", false);
        refreshingLeft = keyTable("LK", true);
        refreshingRight = keyTable("RK", true);
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        executionContext.close();
    }

    private QueryTable keyTable(final String name, final boolean refreshing) {
        final QueryTable table = new QueryTable(RowSetFactory.flat(rows).toTracking(),
                Map.of(name, new ScrambledKeySource(keyCount)));
        table.setRefreshing(refreshing);
        return table;
    }

    @Benchmark
    public long staticBuild() {
        return staticLeft.join(staticRight, "LK=RK").size();
    }

    @Benchmark
    public void leftRefreshingBuild(final Blackhole blackhole) {
        // release the join, and the listener it registers, after each build
        final LivenessScope scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, true)) {
            final Table result =
                    updateGraph.sharedLock().computeLocked(() -> refreshingLeft.join(staticRight, "LK=RK"));
            blackhole.consume(result.size());
        }
    }

    @Benchmark
    public void bothRefreshingBuild(final Blackhole blackhole) {
        // release the join, and the listeners it registers, after each build
        final LivenessScope scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, true)) {
            final Table result =
                    updateGraph.sharedLock().computeLocked(() -> refreshingLeft.join(refreshingRight, "LK=RK"));
            blackhole.consume(result.size());
        }
    }

    public static void main(String[] args) {
        BenchUtil.run(CrossJoinBuildBenchmark.class);
    }
}
