//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.newTable;
import static org.junit.Assert.assertTrue;

/**
 * A match filter on a {@link BigDecimal} column matches by {@link BigDecimal#compareTo(BigDecimal)}, as the query
 * language's {@code ==} does, so the scale of neither the value nor the column's values matters. Sorted columns, which
 * are searched rather than scanned, agree.
 */
public class BigDecimalMatchFilterTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private Table unsorted;

    @Before
    public void setUp() {
        QueryScope.addParam("d", 5.0);
        QueryScope.addParam("l", 5L);
        QueryScope.addParam("bd", new BigDecimal("5.00"));
        unsorted = newTable(col("X", new BigDecimal("5"), new BigDecimal("5.5"), new BigDecimal("5.0"), null,
                new BigDecimal("6.000"), new BigDecimal("5.00"), new BigDecimal("7")));
    }

    /**
     * Asserts that {@code filter} selects from {@code table} what the query language's {@code condition} selects.
     */
    private static void assertSelects(final Table table, final String filter, final String condition) {
        final Table expected = table.where(ConditionFilter.createConditionFilter(condition));
        assertTableEquals(filter, expected, table.where(filter));
    }

    private void assertMatchesIgnoreScale(final Table table) {
        for (final String filter : new String[] {"X == 5.0", "X == 5", "X != 5.0", "X == d", "X == l", "X == bd",
                "X != bd"}) {
            assertSelects(table, filter, filter);
        }
        assertSelects(table, "X in 5.0, 6", "X == 5.0 || X == 6");
        assertSelects(table, "X in 5.0, 6, 7.00", "X == 5.0 || X == 6 || X == 7.00");
        // more than three values, and null among them
        assertSelects(table, "X in 5.0, 6, 7.00, null", "X == 5.0 || X == 6 || X == 7.00 || isNull(X)");
        assertSelects(table, "X not in 5.0, 6, 7.00, null", "!(X == 5.0 || X == 6 || X == 7.00 || isNull(X))");
        assertSelects(table, "X not in 5.0", "!(X == 5.0)");
        assertSelects(table, "X in null", "isNull(X)");
        assertTableEquals(table.where("X == 5.0"),
                table.where(new MatchFilter(MatchOptions.REGULAR, "X", new BigDecimal("5.000"))));
    }

    @Test
    public void matchIgnoresScale() {
        assertMatchesIgnoreScale(unsorted);
    }

    @Test
    public void sortedMatchIgnoresScale() {
        final Table ascending = unsorted.sort("X");
        assertTrue(SortedColumnsAttribute.getOrderForColumn(ascending, "X").isPresent());
        assertMatchesIgnoreScale(ascending);

        final Table descending = unsorted.sortDescending("X");
        assertTrue(SortedColumnsAttribute.getOrderForColumn(descending, "X").isPresent());
        assertMatchesIgnoreScale(descending);
    }
}
