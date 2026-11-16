//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.openjdk.jmh.annotations.*;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

/**
 * Measures the initial {@code update()} and {@code select()} of a sparse, refreshing table for each way the result can
 * be stored.
 *
 * <ul>
 * <li>Positional: the parent is append-only, so the result is dense and redirected by row position.</li>
 * <li>Sparse: the parent is not append-only, so the result is a sparse array source indexed by row key.</li>
 * <li>Hash: as Sparse, but with QueryTable.redirectUpdate and redirectSelect, so the result is dense and redirected
 * through a hash table.</li>
 * <li>Flatten: the parent is flattened first, so the result is a dense array source of a flat table.</li>
 * </ul>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class PositionalSelectBenchmark {
    static final String FORMULA = "X = V1 * 2.0";

    @Param({"Positional", "Sparse", "Hash", "Flatten"})
    private String mode;

    /** Which rows of the parent survive the filter. */
    @Param({"Stride8", "Runs64", "Stride64"})
    private String pattern;

    /** The parent size, and whether parallel population is enabled. */
    @Param({"20M", "160M", "160M-serial"})
    private String config;

    private Table source;

    @Setup(Level.Trial)
    public void setupTrial() {
        final boolean parallel = !config.endsWith("-serial");
        final long parentSize = Long.parseLong(config.substring(0, config.indexOf('M'))) * 1_000_000L;
        setQueryTableFlag("ENABLE_PARALLEL_SELECT_AND_UPDATE", parallel);
        source = createSource(mode, pattern, parentSize);
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final Table result = locked(() -> source.update(FORMULA));
            System.out.println("\nmode=" + mode + ", pattern=" + pattern + ", config=" + config + ", rows="
                    + result.size() + ", X=" + describe(result.getColumnSource("X")));
        }
    }

    @Benchmark
    public long initialUpdate() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            return locked(() -> source.update(FORMULA)).size();
        }
    }

    @Benchmark
    public long initialSelect() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            return locked(() -> source.select(FORMULA)).size();
        }
    }

    /**
     * Build a refreshing, non-flat table that never ticks, filtered from a parent of {@code parentSize} rows.
     */
    static Table createSource(final String mode, final String pattern, final long parentSize) {
        TestExecutionContext.createForUnitTests().open();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();

        setQueryTableFlag("USE_REDIRECTED_COLUMNS_FOR_UPDATE", mode.equals("Hash"));
        setQueryTableFlag("USE_REDIRECTED_COLUMNS_FOR_SELECT", mode.equals("Hash"));
        // dh-tests.prop lowers this to 1024; measure with the production default
        QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = 1L << 22;

        final Table base = TableTools.emptyTable(parentSize).update("Pos = ii", "Key = (int) (ii % 20000)",
                "V1 = ii * 1.0");
        final QueryTable parent = new QueryTable(base.getRowSet().copy().toTracking(), base.getColumnSourceMap());
        parent.setRefreshing(true);
        if (mode.equals("Positional")) {
            parent.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
        }

        final String filter;
        switch (pattern) {
            case "Stride8":
                filter = "Pos % 8 == 0";
                break;
            case "Runs64":
                filter = "Pos % 512 < 64";
                break;
            case "Stride64":
                filter = "Pos % 64 == 0";
                break;
            default:
                throw new IllegalArgumentException("Unknown pattern " + pattern);
        }
        final Table filtered = locked(() -> parent.where(filter));
        if (mode.equals("Flatten")) {
            return locked(filtered::flatten);
        }
        return filtered;
    }

    static <T> T locked(final java.util.function.Supplier<T> operation) {
        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(operation::get);
    }

    static String describe(final ColumnSource<?> columnSource) {
        if (columnSource instanceof RedirectedColumnSource) {
            return columnSource.getClass().getSimpleName() + "("
                    + ((RedirectedColumnSource<?>) columnSource).getRowRedirection().getClass().getSimpleName() + ")";
        }
        return columnSource.getClass().getSimpleName();
    }

    static void setQueryTableFlag(final String name, final boolean value) {
        try {
            final Field field = QueryTable.class.getDeclaredField(name);
            field.setAccessible(true);
            field.setBoolean(null, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to set QueryTable." + name, e);
        }
    }

    public static void main(final String[] args) {
        BenchUtil.run(32, PositionalSelectBenchmark.class);
    }
}
