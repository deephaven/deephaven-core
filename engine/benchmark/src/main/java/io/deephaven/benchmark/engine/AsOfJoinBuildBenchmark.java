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
 * Measures the initial build of an as-of join for each combination of the inputs refreshing:
 * <ul>
 * <li>{@link #staticBuild()}: both static.</li>
 * <li>{@link #leftRefreshingBuild}: the left refreshes, so the join keeps the right rows of each key to restamp left
 * updates.</li>
 * <li>{@link #rightRefreshingBuild}: the right refreshes, so the join keeps the left rows of each key.</li>
 * <li>{@link #bothRefreshingBuild}: both refresh, so the join keeps the rows of every key from either side.</li>
 * </ul>
 * Each side holds {@code keyCount} distinct keys, of which {@code hitPercent} percent occur on the other side, as
 * {@link JoinBenchmarkKeys} builds them; so that percentage of each side's rows find their key on the other side, at
 * every key count. Each side has at least a million rows, and one per key beyond that. Each row's stamp is its row key,
 * so the rows of a key are in stamp order.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class AsOfJoinBuildBenchmark {

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

        final int rows = sideRows(keyCount);
        final ColumnSource<Long> leftKeys = keySource(keys(rows, keyCount, hitPercent, LEFT));
        final ColumnSource<Long> rightKeys = keySource(keys(rows, keyCount, hitPercent, RIGHT));
        staticLeft = sideTable("L", rows, leftKeys, false);
        staticRight = sideTable("R", rows, rightKeys, false);
        refreshingLeft = sideTable("L", rows, leftKeys, true);
        refreshingRight = sideTable("R", rows, rightKeys, true);
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        executionContext.close();
    }

    /**
     * @return a flat table of {@code rows} rows with the key column {@code <prefix>K} and the stamp column
     *         {@code <prefix>S}
     */
    static QueryTable sideTable(final String prefix, final int rows, final ColumnSource<Long> keys,
            final boolean refreshing) {
        return table(rows, Map.of(prefix + "K", keys, prefix + "S", new JoinBenchmarkKeys.RowKeySource()), refreshing);
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
