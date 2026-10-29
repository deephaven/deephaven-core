//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.util.TableTools;
import org.openjdk.jmh.annotations.*;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

/**
 * Measures a {@link MatchFilter} on the key column of a {@code selectDistinct} result, matching a few of its keys.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class AggregationKeyMatchFilterBenchmark {

    @Param({"Int", "String"})
    private String keyType;

    @Param({"100000", "1000000", "10000000", "100000000"})
    private int distinctKeys;

    @Param({"1", "2", "4", "8", "16"})
    private int matchValues;

    private Table distinct;
    private MatchFilter filter;

    @Setup(Level.Trial)
    public void setupTrial() {
        TestExecutionContext.createForUnitTests().open();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();
        QueryTable.setMemoizeResults(false);
        // dh-tests.prop forces two-segment parallel where; measure with the production defaults
        setQueryTableField("FORCE_PARALLEL_WHERE", false);
        setQueryTableField("PARALLEL_WHERE_SEGMENTS", -1);

        // Multiplying by an odd constant permutes the int keys, so the distinct keys are not in row order.
        final String keyFormula = keyType.equals("Int")
                ? "Key = (int) (ii * 2654435761L)"
                : "Key = Integer.toString((int) (ii * 2654435761L))";
        distinct = TableTools.emptyTable(distinctKeys).update(keyFormula).selectDistinct("Key");

        final ColumnSource<?> keySource = distinct.getColumnSource("Key");
        final Object[] values = new Object[matchValues];
        for (int ii = 0; ii < matchValues; ++ii) {
            values[ii] = keySource.get(distinct.getRowSet().get((long) ii * distinctKeys / matchValues));
        }
        filter = new MatchFilter(MatchOptions.REGULAR, "Key", values);

        final long matched = distinct.where(filter).size();
        System.out.println("\nkeyType=" + keyType + ", distinctKeys=" + distinct.size() + ", matched=" + matched);
        if (distinct.size() != distinctKeys || matched != matchValues) {
            throw new IllegalStateException("Expected " + distinctKeys + " keys and " + matchValues + " matches");
        }
    }

    @Benchmark
    public long where() {
        return distinct.where(filter).size();
    }

    private static void setQueryTableField(final String name, final Object value) {
        try {
            final Field field = QueryTable.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(null, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to set QueryTable." + name, e);
        }
    }

    public static void main(final String[] args) {
        BenchUtil.run(AggregationKeyMatchFilterBenchmark.class);
    }
}
