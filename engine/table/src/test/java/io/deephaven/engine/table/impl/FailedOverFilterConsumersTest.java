//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.RangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static org.junit.Assert.assertEquals;
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
        // 1.5 cannot be read as an int, so the filter fails over
        final Table t = newTable(intCol("X", 1, 2));
        final WhereFilter filter = initialized(t, "X == 1.5");
        assertNotNull(((MatchFilter) filter).getFailoverFilter());

        try (final BasePushdownFilterContextImpl context =
                new BasePushdownFilterContextImpl(filter, List.of(t.getColumnSource("X")))) {
            assertTrue(context.supportsChunkFiltering());
        }
    }

    @Test
    public void pushdownChunkFilteringSeesARangeFilterFailover() {
        // 1.5 cannot be read as an int, so the filter fails over
        final Table t = newTable(intCol("X", 1, 2));
        final WhereFilter filter = initialized(t, "X < 1.5");
        assertTrue(((RangeFilter) filter).getRealFilter() instanceof ConditionFilter);

        try (final BasePushdownFilterContextImpl context =
                new BasePushdownFilterContextImpl(filter, List.of(t.getColumnSource("X")))) {
            assertTrue(context.supportsChunkFiltering());
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
