//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import java.util.*;
import java.util.stream.Collectors;

import io.deephaven.api.ColumnName;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

public class KeyedTransposeTest extends RefreshingTableTestCase {
    final Table staticSource = TableTools.newTable(
            stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07",
                    "2025-08-08", "2025-08-08"),
            stringCol("Host", "h1", "h1", "h2", "h1", "h2", "h2", "h2"),
            stringCol("Level", "INFO", "INFO", "WARN", "ERROR", "INFO", "WARN", "WARN"),
            intCol("Cat", 1, 1, 2, 3, 3, 2, 1),
            floatCol("BadInt", 0.1f, 20, NULL_FLOAT, -30, 20, 0.1f, NULL_FLOAT),
            stringCol("BadStr", "A-B", "C D", "E.F", "A-B", "C D", NULL_STRING, "C D"));

    /**
     * Test the JavaDoc example for {@link KeyedTranspose} method.
     */
    public void testJavaDocExample() {
        Table staticSource = TableTools.newTable(
                stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07"),
                stringCol("Level", "INFO", "INFO", "WARN", "ERROR"));
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count")),
                colsOf("Date"), colsOf("Level"));
        Table ex = TableTools.newTable(stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-07"),
                longCol("INFO", 2, NULL_LONG, NULL_LONG),
                longCol("WARN", NULL_LONG, 1, NULL_LONG),
                longCol("ERROR", NULL_LONG, NULL_LONG, 1));
        assertFalse(t.isRefreshing());
        assertTableEquals(ex, t);
    }

    public void testOneAggOneByColWithInitialGroups() {
        Table initialGroups = TableTools.newTable(stringCol("Level", "ERROR", "WARN", "INFO"))
                .join(staticSource.selectDistinct("Date", "Host"));
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count")), colsOf("Date", "Host"),
                colsOf("Level"), initialGroups);
        Table ex = TableTools.newTable(
                stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07", "2025-08-08"),
                stringCol("Host", "h1", "h2", "h1", "h2", "h2"),
                longCol("ERROR", 0, 0, 1, 0, 0),
                longCol("WARN", 0, 1, 0, 0, 2),
                longCol("INFO", 2, 0, 0, 1, 0));
        assertFalse(t.isRefreshing());
        assertTableEquals(ex, t);
    }

    public void testOneAggOneByColNoInitialGroups() {
        Table initialGroups = TableTools.emptyTable(0);
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count")), colsOf("Date", "Host"),
                colsOf("Level"), initialGroups);
        Table ex = TableTools.newTable(
                stringCol("Date", "2025-08-05", "2025-08-07", "2025-08-06", "2025-08-08", "2025-08-07"),
                stringCol("Host", "h1", "h2", "h2", "h2", "h1"),
                longCol("INFO", 2, 1, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("WARN", NULL_LONG, NULL_LONG, 1, 2, NULL_LONG),
                longCol("ERROR", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1));
        assertFalse(t.isRefreshing());
        assertTableEquals(ex, t);
    }

    public void testTwoAggOneByCol() {
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                colsOf("Date", "Host"), colsOf("Level"));
        Table ex = TableTools.newTable(
                stringCol("Date", "2025-08-05", "2025-08-07", "2025-08-06", "2025-08-08", "2025-08-07"),
                stringCol("Host", "h1", "h2", "h2", "h2", "h1"),
                longCol("Count_INFO", 2, 1, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Sum_INFO", 2, 3, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_WARN", NULL_LONG, NULL_LONG, 1, 2, NULL_LONG),
                longCol("Sum_WARN", NULL_LONG, NULL_LONG, 2, 3, NULL_LONG),
                longCol("Count_ERROR", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1),
                longCol("Sum_ERROR", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 3));
        assertFalse(t.isRefreshing());
        assertTableEquals(ex, t);
    }

    public void testOneAggTwoByCol() {
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count")),
                colsOf("Date", "Host"), colsOf("Level", "Cat"));
        Table ex = TableTools.newTable(
                stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-08", "2025-08-07", "2025-08-07"),
                stringCol("Host", "h1", "h2", "h2", "h1", "h2"),
                longCol("INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("WARN_2", NULL_LONG, 1, 1, NULL_LONG, NULL_LONG),
                longCol("ERROR_3", NULL_LONG, NULL_LONG, NULL_LONG, 1, NULL_LONG),
                longCol("INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1),
                longCol("WARN_1", NULL_LONG, NULL_LONG, 1, NULL_LONG, NULL_LONG));
        assertFalse(t.isRefreshing());
        assertTableEquals(ex, t);
    }

    public void testTwoAggTwoByColWithInitialGroups() {
        Table initialGroups = TableTools.newTable(stringCol("Level", "INFO", "WARN", "ERROR"),
                intCol("Cat", 3, 2, 1)).join(staticSource.selectDistinct("Date", "Host"));
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                colsOf("Date", "Host"), colsOf("Level", "Cat"), initialGroups);
        Table ex = TableTools.newTable(
                stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07", "2025-08-08"),
                stringCol("Host", "h1", "h2", "h1", "h2", "h2"),
                longCol("Count_INFO_3", 0, 0, 0, 1, 0),
                longCol("Sum_INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, 3, NULL_LONG),
                longCol("Count_WARN_2", 0, 1, 0, 0, 1),
                longCol("Sum_WARN_2", NULL_LONG, 2, NULL_LONG, NULL_LONG, 2),
                longCol("Count_ERROR_1", 0, 0, 0, 0, 0),
                longCol("Sum_ERROR_1", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Sum_INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_ERROR_3", NULL_LONG, NULL_LONG, 1, NULL_LONG, NULL_LONG),
                longCol("Sum_ERROR_3", NULL_LONG, NULL_LONG, 3, NULL_LONG, NULL_LONG),
                longCol("Count_WARN_1", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1),
                longCol("Sum_WARN_1", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1));
        assertFalse(t.isRefreshing());
        assertTableEquals(ex, t);
    }

    public void testTwoAggTwoByColNoInitialGroups() {
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                colsOf("Date", "Host"), colsOf("Level", "Cat"));
        Table ex = TableTools.newTable(
                stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-08", "2025-08-07", "2025-08-07"),
                stringCol("Host", "h1", "h2", "h2", "h1", "h2"),
                longCol("Count_INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Sum_INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_WARN_2", NULL_LONG, 1, 1, NULL_LONG, NULL_LONG),
                longCol("Sum_WARN_2", NULL_LONG, 2, 2, NULL_LONG, NULL_LONG),
                longCol("Count_ERROR_3", NULL_LONG, NULL_LONG, NULL_LONG, 1, NULL_LONG),
                longCol("Sum_ERROR_3", NULL_LONG, NULL_LONG, NULL_LONG, 3, NULL_LONG),
                longCol("Count_INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1),
                longCol("Sum_INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 3),
                longCol("Count_WARN_1", NULL_LONG, NULL_LONG, 1, NULL_LONG, NULL_LONG),
                longCol("Sum_WARN_1", NULL_LONG, NULL_LONG, 1, NULL_LONG, NULL_LONG));
        assertFalse(t.isRefreshing());
        assertTableEquals(ex, t);
    }

    public void testTwoAggTwoByColEmptySource() {
        Table initialGroups = TableTools.newTable(stringCol("Level", "INFO", "WARN", "ERROR"),
                intCol("Cat", 3, 2, 1)).join(staticSource.selectDistinct("Date", "Host"));
        Table emptySource = staticSource.where("Date == `No Match`");
        Table t = KeyedTranspose.keyedTranspose(emptySource, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                colsOf("Date", "Host"), colsOf("Level", "Cat"), initialGroups);
        Table ex = TableTools.newTable(
                stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07", "2025-08-08"),
                stringCol("Host", "h1", "h2", "h1", "h2", "h2"),
                longCol("Count_INFO_3", 0, 0, 0, 0, 0),
                longCol("Sum_INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_WARN_2", 0, 0, 0, 0, 0),
                longCol("Sum_WARN_2", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_ERROR_1", 0, 0, 0, 0, 0),
                longCol("Sum_ERROR_1", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG));
        assertFalse(t.isRefreshing());
        assertTableEquals(ex, t);
    }

    public void testOneAggOneByColWithIllegalValues() {
        Table initialGroups = TableTools.newTable(floatCol("BadInt", 0.1f, 20, NULL_FLOAT, -30),
                stringCol("BadStr", "A-B", "C D", "E.F", NULL_STRING))
                .join(staticSource.selectDistinct("Date"));
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count")), colsOf("Date"),
                colsOf("BadInt", "BadStr"), initialGroups);
        Table ex = TableTools.newTable(stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-08"),
                longCol("column_01_AB", 1, 0, 0, 0),
                longCol("column_200_CD", 1, 0, 1, 0),
                longCol("null_EF", 0, 1, 0, 0),
                longCol("column_300_null", 0, 0, 0, 0),
                longCol("column_300_AB", NULL_LONG, NULL_LONG, 1, NULL_LONG),
                longCol("column_01_null", NULL_LONG, NULL_LONG, NULL_LONG, 1),
                longCol("null_CD", NULL_LONG, NULL_LONG, NULL_LONG, 1));
        assertFalse(t.isRefreshing());
        assertTableEquals(ex, t);
    }

    public void testEmptyTableForSource() {
        Table emptySource = TableTools.newTable(stringCol("Date"), stringCol("Level"));
        try {
            Table t = KeyedTranspose.keyedTranspose(emptySource, List.of(AggCount("Count")), colsOf("Date"),
                    colsOf("Level"));
            fail("Should have thrown an exception");
        } catch (Exception ex) {
            ;
            assertTrue("Wrong message: " + ex.getMessage(),
                    ex.getMessage().contains("table has no values for column by columns"));
        }
    }

    public void testNewRow() {
        testNewRow(KeyedTranspose.NewColumnBehavior.IGNORE);
        testNewRow(KeyedTranspose.NewColumnBehavior.FAIL);
    }

    private void testNewRow(final KeyedTranspose.NewColumnBehavior newColumnBehavior) {
        final QueryTable source =
                TstUtils.testRefreshingTable(RowSetFactory.flat(7).toTracking(), intCol("Row", 1, 2, 3, 2, 3, 2, 3),
                        stringCol("Col", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Alpha", "Alpha"),
                        doubleCol("Value", 10.1, 20.2, 30.3, 40.4, 50.5, 60.6, 70.7));
        final Table result = KeyedTranspose.keyedTranspose(source, List.of(AggSum("Value")), colsOf("Row"),
                colsOf("Col"), null, newColumnBehavior);
        assertTrue(result.isRefreshing());

        final ErrorListener el = new ErrorListener(result);
        result.addUpdateListener(el);

        final TableUpdateValidator validator = TableUpdateValidator.make("keyedTranspose", (QueryTable) result);
        final Table validated = validator.getResultTable();

        final Table expected = TableTools.newTable(intCol("Row", 1, 2, 3),
                doubleCol("Alpha", 10.1, 20.2 + 60.6, 30.3 + 70.7), doubleCol("Bravo", NULL_DOUBLE, 40.4, 50.5));
        assertTableEquals(expected, validated);

        // adding a fourth row is totally fine under any circumstance
        final ControlledUpdateGraph cug = ExecutionContext.getContext().getUpdateGraph().cast();
        cug.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(10, 11), intCol("Row", 4, 4), stringCol("Col", "Alpha", "Bravo"),
                    doubleCol("Value", 101.1, 102.2));
            source.notifyListeners(i(10, 11), i(), i());
        });
        validator.validate();

        final Table expected2 = TableTools.merge(expected,
                TableTools.newTable(intCol("Row", 4), doubleCol("Alpha", 101.1), doubleCol("Bravo", 102.2)));
        assertTableEquals(expected2, validated);

        if (newColumnBehavior == KeyedTranspose.NewColumnBehavior.IGNORE) {
            // The new column Charlie is ignored; Alpha and Bravo get updated
            cug.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, i(10, 11), intCol("Row", 4, 4), stringCol("Col", "Charlie", "Bravo"),
                        doubleCol("Value", 201.1, 202.2));
                source.notifyListeners(i(), i(), i(10, 11));
            });
            validator.validate();

            final Table expected3 = TableTools.merge(expected,
                    TableTools.newTable(intCol("Row", 4), doubleCol("Alpha", NULL_DOUBLE), doubleCol("Bravo", 202.2)));
            assertTableEquals(expected3, validated);
            assertNull(el.originalException());
        } else {
            // we are going to fail with this update
            setExpectError(true);

            // The new column Charlie is ignored; Alpha and Bravo get updated
            cug.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, i(10, 11), intCol("Row", 4, 4), stringCol("Col", "Charlie", "Bravo"),
                        doubleCol("Value", 201.1, 202.2));
                source.notifyListeners(i(), i(), i(10, 11));
            });
            assertTrue(result.isFailed());
            assertNotNull(el.originalException());
            final Throwable exception = el.originalException();
            assertTrue(exception instanceof IllegalStateException);
            assertEquals(
                    "New constituent table detected in keyedTranspose; consider setting newColumnBehavior to Ignore.",
                    exception.getMessage());
        }
    }

    private Collection<? extends ColumnName> colsOf(String... cols) {
        return Arrays.stream(cols).map(ColumnName::of).collect(Collectors.toList());
    }

}
