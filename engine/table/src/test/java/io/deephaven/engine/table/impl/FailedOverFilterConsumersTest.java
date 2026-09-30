//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.RawString;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.RangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * A {@link MatchFilter} or {@link RangeFilter} that fails over is evaluated as its {@link ConditionFilter}, and code
 * that treats a {@link ConditionFilter} specially must treat it so too.
 */
public class FailedOverFilterConsumersTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static WhereFilter initialized(final Table table, final String expression) {
        final WhereFilter filter = WhereFilterFactory.getExpression(expression);
        filter.init(table.getDefinition());
        return filter;
    }

    @Test
    public void pushdownChunkFilteringSeesAMatchFilterFailover() {
        // the query language's == rounds the largest longs to 2^63, so the filter fails over
        QueryScope.addParam("failoverVal", 0x1p63);
        final Table t = newTable(longCol("X", 1L, Long.MAX_VALUE));
        final WhereFilter filter = initialized(t, "X == failoverVal");
        assertNotNull(((MatchFilter) filter).getFailoverFilter());

        try (final BasePushdownFilterContextImpl context =
                new BasePushdownFilterContextImpl(filter, List.of(t.getColumnSource("X")))) {
            assertTrue(context.supportsChunkFiltering());
        }
    }

    @Test
    public void pushdownChunkFilteringSeesARangeFilterFailover() {
        // the query language compares a double with a BigDecimal through BigDecimal.valueOf, so the filter fails over
        QueryScope.addParam("failoverVal", new BigDecimal("1.5"));
        final Table t = newTable(doubleCol("D", 1.0, 2.0));
        final WhereFilter filter = initialized(t, "D < failoverVal");
        assertTrue(((RangeFilter) filter).getRealFilter() instanceof ConditionFilter);

        try (final BasePushdownFilterContextImpl context =
                new BasePushdownFilterContextImpl(filter, List.of(t.getColumnSource("D")))) {
            assertTrue(context.supportsChunkFiltering());
        }
    }

    @Test
    public void pushdownRefusesAFailoverThatDoesNotPermitParallelization() {
        QueryScope.addParam("failoverLong", 0x1p63);
        QueryScope.addParam("failoverDecimal", new BigDecimal("1.5"));
        final Table longs = newTable(longCol("X", 1L, Long.MAX_VALUE));
        final Table doubles = newTable(doubleCol("X", 1.0, 2.0));
        final boolean statelessByDefault = QueryTable.STATELESS_FILTERS_BY_DEFAULT;
        QueryTable.STATELESS_FILTERS_BY_DEFAULT = false;
        try {
            // each fails over to a ConditionFilter, which is not stateless by default now, so neither may be pushed
            // down
            for (final Table t : new Table[] {longs, doubles}) {
                final String expression = t == longs ? "X == failoverLong" : "X < failoverDecimal";
                final WhereFilter filter = initialized(t, expression);
                assertFalse(expression, filter.permitParallelization());
                assertThrows(expression, IllegalArgumentException.class,
                        () -> new BasePushdownFilterContextImpl(filter, List.of(t.getColumnSource("X"))));
            }
        } finally {
            QueryTable.STATELESS_FILTERS_BY_DEFAULT = statelessByDefault;
        }
    }

    /**
     * A refreshing table that is not append-only may not use {@code i}. The failover of {@code X == i} and of
     * {@code X < i} must be refused as the ConditionFilter it is. This asks the filter directly: the table operations
     * that ask it refuse {@code i}, {@code ii} and {@code k} outright first.
     */
    @Test
    public void refreshSafetySeesAFailover() {
        final BaseTable<?> t = (BaseTable<?>) TstUtils.testRefreshingTable(intCol("X", 0, 1, 5))
                .withoutAttributes(List.of(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE));
        for (final String expression : new String[] {"X == i + 0", "X == i", "X < i + 0", "X < i"}) {
            final WhereFilter filter = initialized(t, expression);
            final IllegalArgumentException err = assertThrows(expression, IllegalArgumentException.class,
                    () -> filter.validateSafeForRefresh(t));
            assertTrue(expression, err.getMessage().contains("is not safe to refresh"));
        }
    }

    /**
     * {@code X == i} fails over to a ConditionFilter that uses {@code i}. Count-where refuses such a ConditionFilter,
     * rather than evaluating {@code i} against its own chunks, and so must refuse the MatchFilter.
     */
    @Test
    public void countWhereRefusesAFailoverThatUsesVirtualRowVariables() {
        final Table t = newTable(intCol("X", 0, 1, 5));

        final Exception plain =
                assertThrows(Exception.class, () -> t.aggBy(Aggregation.AggCountWhere("C", "X == i + 0")));
        final Exception failedOver =
                assertThrows(Exception.class, () -> t.aggBy(Aggregation.AggCountWhere("C", "X == i")));
        assertEquals(rootCause(plain).getMessage(), rootCause(failedOver).getMessage());
    }

    /** A wrapper or a composed filter answers for the filters inside it, and so must be refused too. */
    @Test
    public void countWhereRefusesWrappedAndComposedVirtualRowVariables() {
        final Table t = newTable(intCol("X", 0, 1, 5));

        final Exception plain =
                assertThrows(Exception.class, () -> t.aggBy(Aggregation.AggCountWhere("C", "X == i + 0")));
        for (final Filter filter : new Filter[] {Filter.serial(RawString.of("X == i")),
                Filter.or(RawString.of("X == 5"), RawString.of("X == i"))}) {
            final Exception agg = assertThrows(filter.toString(), Exception.class,
                    () -> t.aggBy(Aggregation.AggCountWhere("C", filter)));
            assertEquals(rootCause(plain).getMessage(), rootCause(agg).getMessage());
            final Exception updateBy = assertThrows(filter.toString(), Exception.class,
                    () -> t.updateBy(UpdateByOperation.CumCountWhere("C", filter)));
            assertEquals(
                    "UpdateBy CountWhere operator does not support filters that reference virtual row variables (i, ii, k)",
                    rootCause(updateBy).getMessage());
        }
    }

    @Test
    public void updateByCountWhereRefusesAFailoverThatUsesVirtualRowVariables() {
        final Table t = newTable(intCol("X", 0, 1, 5));

        final Exception plain =
                assertThrows(Exception.class, () -> t.updateBy(UpdateByOperation.CumCountWhere("C", "X == i + 0")));
        final Exception failedOver =
                assertThrows(Exception.class, () -> t.updateBy(UpdateByOperation.CumCountWhere("C", "X == i")));
        assertEquals(rootCause(plain).getMessage(), rootCause(failedOver).getMessage());
    }

    private static Throwable rootCause(Throwable err) {
        while (err.getCause() != null) {
            err = err.getCause();
        }
        return err;
    }
}
