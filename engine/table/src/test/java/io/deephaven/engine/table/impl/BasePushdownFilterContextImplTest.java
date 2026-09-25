//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.BasePushdownFilterContext.FilterNullBehavior;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BasePushdownFilterContextImpl}.
 */
public class BasePushdownFilterContextImplTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static FilterNullBehavior nullBehaviorOf(final WhereFilter filter, final ColumnSource<?> source) {
        try (final BasePushdownFilterContextImpl context = new BasePushdownFilterContextImpl(filter, List.of(source))) {
            return context.filterNullBehavior();
        }
    }

    /**
     * The null-behavior probe evaluates a copy of the filter. For a {@link MatchFilter} that failed over to a
     * {@code ConditionFilter}, the copy must still evaluate correctly; otherwise the probe throws and reports
     * {@link FilterNullBehavior#FAILS_ON_NULLS}, disabling null-aware pushdown for the filter.
     */
    @Test
    public void testFilterNullBehaviorOfFailoverMatchFilter() {
        final IntegerArraySource source = new IntegerArraySource();
        source.ensureCapacity(1, false);
        source.set(0, 0);
        final QueryTable table = new QueryTable(RowSetFactory.flat(1).toTracking(), Map.of("X", source));

        for (final Map.Entry<String, FilterNullBehavior> entry : Map.of(
                "X == i", FilterNullBehavior.EXCLUDES_NULLS,
                "X != i", FilterNullBehavior.INCLUDES_NULLS).entrySet()) {
            final String expression = entry.getKey();
            final WhereFilter filter = WhereFilterFactory.getExpression(expression);
            filter.init(table.getDefinition());
            assertTrue("sanity: " + expression + " parses to a MatchFilter", filter instanceof MatchFilter);
            assertNotNull("sanity: " + expression + " fails over",
                    ((MatchFilter) filter).getFailoverFilterIfCached());

            assertEquals(expression, entry.getValue(), nullBehaviorOf(filter, source));
        }
    }
}
