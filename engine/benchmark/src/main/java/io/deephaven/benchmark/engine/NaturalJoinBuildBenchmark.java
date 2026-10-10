//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
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

import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.LEFT;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.RIGHT;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.keySource;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.keys;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.sideRows;
import static io.deephaven.benchmark.engine.JoinBenchmarkKeys.table;

/**
 * Measures the initial build of a natural join for each combination of the inputs refreshing:
 * <ul>
 * <li>{@link #staticBuild()}: both static.</li>
 * <li>{@link #leftRefreshingBuild}: the left refreshes, so the join keeps the right keys to probe left updates.</li>
 * <li>{@link #rightRefreshingBuild}: the right refreshes, so the join keeps the left rows of each key.</li>
 * <li>{@link #bothRefreshingBuild}: both refresh, so the join keeps every key from either side.</li>
 * </ul>
 * Each side holds {@code keyCount} distinct keys, of which {@code hitPercent} percent occur on the other side, as
 * {@link JoinBenchmarkKeys} builds them; so that percentage of each side's rows find their key on the other side,
 * whichever side the join builds from and at every key count. The right table has one row per key, as a natural join's
 * right keys are unique; the left has at least a million rows, and one per key beyond that.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class NaturalJoinBuildBenchmark {

    @Param({"100000", "1000000", "10000000"})
    private int keyCount;

    @Param({"25", "75"})
    private int hitPercent;

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

        final int leftRows = sideRows(keyCount);
        final ColumnSource<Long> leftKeys = keySource(keys(leftRows, keyCount, hitPercent, LEFT));
        final ColumnSource<Long> rightKeys = keySource(keys(keyCount, keyCount, hitPercent, RIGHT));
        staticLeft = table(leftRows, Map.of("LK", leftKeys), false);
        staticRight = table(keyCount, Map.of("RK", rightKeys), false);
        refreshingLeft = table(leftRows, Map.of("LK", leftKeys), true);
        refreshingRight = table(keyCount, Map.of("RK", rightKeys), true);
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        executionContext.close();
    }

    @Benchmark
    public long staticBuild() {
        return staticLeft.naturalJoin(staticRight, "LK=RK").size();
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
            final Table result = updateGraph.sharedLock().computeLocked(() -> left.naturalJoin(right, "LK=RK"));
            blackhole.consume(result.size());
        }
    }

    public static void main(String[] args) {
        BenchUtil.run(NaturalJoinBuildBenchmark.class);
    }
}
