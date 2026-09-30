//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        QueryScope.addParam("s", "5");
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
    public void valueOfAnotherTypeMatchesNothing() {
        // a value that is not a BigDecimal can never equal one, so it matches nothing, sorted column or not
        final Object[] values = {5.0, new BigDecimal("7"), "5", null, new BigDecimal("5.0")};
        for (final Table table : new Table[] {unsorted, unsorted.sort("X")}) {
            assertTableEquals(table.where("X == 7 || isNull(X) || X == 5.0"),
                    table.where(new MatchFilter(MatchOptions.REGULAR, "X", values)));
            assertTableEquals(table.where("!(X == 7 || isNull(X) || X == 5.0)"),
                    table.where(new MatchFilter(MatchOptions.INVERTED, "X", values)));
            // and likewise from the query scope, where a String is passed through unconverted
            assertTableEquals(table.where("X == 7 || isNull(X) || X == 5.0"), table.where("X in s, 7, null, 5.0"));
            assertTableEquals(table.where("!(X == 7 || isNull(X) || X == 5.0)"),
                    table.where("X not in s, 7, null, 5.0"));
        }
    }

    @Test
    public void columnSourceMatchSkipsValueOfAnotherType() {
        // ColumnSource.match reaches the chunk filter without going through MatchFilter
        final ColumnSource<?> source = unsorted.getColumnSource("X");
        final Object[] keys = {5.0, new BigDecimal("7"), "5"};
        try (final WritableRowSet matched = source.match(false, MatchOptions.REGULAR, unsorted.getRowSet(), keys);
                final WritableRowSet unmatched =
                        source.match(false, MatchOptions.INVERTED, unsorted.getRowSet(), keys)) {
            // only the 7, the last row, matches
            assertEquals(1, matched.size());
            assertEquals(6, matched.firstRowKey());
            assertEquals(6, unmatched.size());
            assertFalse(unmatched.containsRange(6, 6));
        }
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
