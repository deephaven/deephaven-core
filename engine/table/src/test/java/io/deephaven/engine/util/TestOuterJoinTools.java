//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestOuterJoinTools {
    @Rule
    public EngineCleanup cleanup = new EngineCleanup();

    @Test
    public void testLeftOuterJoin() {
        Table lTable = testRefreshingTable(col("X", "a", "b", "c"));
        Table rTable = testRefreshingTable(col("Y", "a", "b", "b"), intCol("Z", 1, 2, 3));
        Table result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X=Y");
        assertEquals(4, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertEquals("Z", result.getDefinition().getColumnsArray()[2].getName());
        assertArrayEquals(new String[] {"a", "b", "b", "c"},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", "b", null},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());
        assertArrayEquals(new int[] {1, 2, 3, NULL_INT}, ColumnVectors.ofInt(result, "Z").toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b"));
        result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X=Y");
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertArrayEquals(new String[] {"a", "b", "c"}, ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", null}, ColumnVectors.ofObject(result, "Y", String.class).toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("X", "a", "b", "d"));
        result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X");
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(1, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertArrayEquals(new String[] {"a", "b", "c"}, ColumnVectors.ofObject(result, "X", String.class).toArray());
    }

    @Test
    public void testLeftOuterJoinStatic() {
        Table lTable = testTable(col("X", "a", "b", "c"));
        Table rTable = testTable(col("Y", "a", "b", "b"), col("Z", 1, 2, 3));
        Table result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X=Y").select();
        assertEquals(4, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertEquals("Z", result.getDefinition().getColumnsArray()[2].getName());
        assertArrayEquals(new String[] {"a", "b", "b", "c"},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", "b", null},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());
        assertArrayEquals(new int[] {1, 2, 3, NULL_INT}, ColumnVectors.ofInt(result, "Z").toArray());

        lTable = testTable(col("X", "a", "b", "c"));
        rTable = testTable(col("Y", "a", "b"));
        result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X=Y").select();
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertArrayEquals(new String[] {"a", "b", "c"}, ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", null}, ColumnVectors.ofObject(result, "Y", String.class).toArray());

        lTable = testTable(col("X", "a", "b", "c"));
        rTable = testTable(col("X", "a", "b", "d"));
        result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X").select();
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(1, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertArrayEquals(new String[] {"a", "b", "c"}, ColumnVectors.ofObject(result, "X", String.class).toArray());
    }

    @Test
    public void testFullOuterJoin() {
        Table lTable = testRefreshingTable(col("X", "a", "b", "c"));
        Table rTable = testRefreshingTable(col("Y", "a", "b", "b"), intCol("Z", 1, 2, 3));
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        assertEquals(4, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertEquals("Z", result.getDefinition().getColumnsArray()[2].getName());
        assertArrayEquals(new String[] {"a", "b", "b", "c"},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", "b", null},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());
        assertArrayEquals(new int[] {1, 2, 3, NULL_INT}, ColumnVectors.ofInt(result, "Z").toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "d"), intCol("Z", 1, 2, 3));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y", "Y");
        TableTools.showWithRowSet(lTable);
        TableTools.showWithRowSet(rTable);
        TableTools.showWithRowSet(result);
        assertEquals(4, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertArrayEquals(new String[] {"a", "b", "c", null},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", null, "d"},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b"));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertArrayEquals(new String[] {"a", "b", "c"}, ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", null}, ColumnVectors.ofObject(result, "Y", String.class).toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "d"));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        TableTools.show(result);
        assertEquals(4, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertArrayEquals(new String[] {"a", "b", "c", null},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", null, "d"},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "b"));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        TableTools.show(result);
        assertEquals(4, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertArrayEquals(new String[] {"a", "b", "b", "c"},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", "b", null},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("X", "a", "b", "d"));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X");
        TableTools.show(result);
        assertEquals(4, result.size());
        assertEquals(1, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertArrayEquals(new String[] {"a", "b", "c", "d"},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
    }

    @Test
    public void testFullOuterJoinAddColumnsDoesNotIncludeRightMatchColumns() {
        Table lTable = testRefreshingTable(col("X", "a", "b", "c"));
        Table rTable = testRefreshingTable(col("Y", "a", "b", "d"), intCol("Z", 1, 2, 3));
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y", "A=Y");
        assertEquals(4, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("A", result.getDefinition().getColumnsArray()[1].getName());
        assertArrayEquals(new String[] {"a", "b", "c", null},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", null, "d"},
                ColumnVectors.ofObject(result, "A", String.class).toArray());
    }

    @Test
    public void testFullOuterJoinAddColumnRenamesToOverrideMatchColumnName() {
        Table lTable = testRefreshingTable(col("X", "a", "b", "c"));
        Table rTable = testRefreshingTable(col("Y", "a", "b", "d"), col("Z", "e", "f", "g"));
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y", "Y=Z");
        assertEquals(4, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertArrayEquals(new String[] {"a", "b", "c", null},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"e", "f", null, "g"},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());
    }

    @Test
    public void testFullOuterJoinMultipleMatchesBothSides() {
        Table lTable = testRefreshingTable(col("X", "a", "a", "b", "c"));
        Table rTable = testRefreshingTable(
                col("Y", "a", "b", "b", "d"),
                intCol("Z", 1, 2, 3, 4));
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        assertEquals(6, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertEquals("Z", result.getDefinition().getColumnsArray()[2].getName());
        assertArrayEquals(new String[] {"a", "a", "b", "b", "c", null},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "a", "b", "b", null, "d"},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());
        assertArrayEquals(new int[] {1, 1, 2, 3, NULL_INT, 4}, ColumnVectors.ofInt(result, "Z").toArray());

        lTable = testRefreshingTable(col("X", "a", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "b", "d"), intCol("Z", 1, 2, 3, 4));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y", "Y");
        TableTools.showWithRowSet(lTable);
        TableTools.showWithRowSet(rTable);
        TableTools.showWithRowSet(result);
        assertEquals(6, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("Y", result.getDefinition().getColumnsArray()[1].getName());
        assertArrayEquals(new String[] {"a", "a", "b", "b", "c", null},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "a", "b", "b", null, "d"},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());
    }

    @Test
    public void testFullOuterJoinIdentityMatchWithAddColumn() {
        Table lTable = TableTools.emptyTable(4).update("a = i", "b = i * 2");
        Table rTable = TableTools.emptyTable(4).update("a = i", "c = i * 3");
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "a", "c");

        assertEquals(4, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("a", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("b", result.getDefinition().getColumnsArray()[1].getName());
        assertEquals("c", result.getDefinition().getColumnsArray()[2].getName());
        assertArrayEquals(new int[] {0, 1, 2, 3}, ColumnVectors.ofInt(result, "a").toArray());
        assertArrayEquals(new int[] {0, 2, 4, 6}, ColumnVectors.ofInt(result, "b").toArray());
        assertArrayEquals(new int[] {0, 3, 6, 9}, ColumnVectors.ofInt(result, "c").toArray());
    }

    @Test
    public void testStaticEmptyRightZeroKey() {
        final Table lhs = TableTools.emptyTable(5).update("I=i");
        final Table rhs = TableTools.emptyTable(0).update("J=`asdf`");

        Table result = OuterJoinTools.leftOuterJoin(lhs, rhs, Collections.emptyList());
        Table expected = lhs.update("J=(String)(null)");
        assertTableEquals(expected, result);

        result = OuterJoinTools.fullOuterJoin(lhs, rhs, Collections.emptyList());
        expected = lhs.update("J=(String)(null)");
        assertTableEquals(expected, result);
    }

    @Test
    public void testDynamicRightLeftOuterJoinZeroKey() {
        final Table lhs = testRefreshingTable(intCol("I", 0, 1));
        final QueryTable rhsSource = testRefreshingTable(stringCol("J"));

        // This update creates a SingleValueColumnSource for the RHS table, which causes problems if we query the
        // column source for a value that doesn't exist in the table.
        final Table rhs = rhsSource.update("J = `asdf`");

        final Table result = OuterJoinTools.leftOuterJoin(lhs, rhs, Collections.emptyList());

        assertEquals(2, result.numColumns());
        assertEquals("I", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("J", result.getDefinition().getColumnsArray()[1].getName());

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new String[] {null, null}, ColumnVectors.ofObject(result, "J", String.class).toArray());

        ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Add some matching and non-matching rows to RHS
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newKeys = i(10);
            TstUtils.addToTable(rhsSource, newKeys, stringCol("J", "anything"));
            rhsSource.notifyListeners(newKeys, RowSetFactory.empty(), RowSetFactory.empty());
        });

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new String[] {"asdf", "asdf"}, ColumnVectors.ofObject(result, "J", String.class).toArray());

        // Remove all rows from RHS
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removedKeys = i(10);
            TstUtils.removeRows(rhsSource, removedKeys);
            rhsSource.notifyListeners(RowSetFactory.empty(), removedKeys, RowSetFactory.empty());
        });

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new String[] {null, null}, ColumnVectors.ofObject(result, "J", String.class).toArray());
    }

    @Test
    public void testDynamicRightFullOuterJoinZeroKey() {
        final Table lhs = testRefreshingTable(intCol("I", 0, 1));
        final QueryTable rhsSource = testRefreshingTable(stringCol("J"));

        // This update creates a SingleValueColumnSource for the RHS table, which causes problems if we query the
        // column source for a value that doesn't exist in the table.
        final Table rhs = rhsSource.update("J = `asdf`");

        final Table result = OuterJoinTools.fullOuterJoin(lhs, rhs, Collections.emptyList());

        assertEquals(2, result.numColumns());
        assertEquals("I", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("J", result.getDefinition().getColumnsArray()[1].getName());

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new String[] {null, null}, ColumnVectors.ofObject(result, "J", String.class).toArray());

        ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Add some matching and non-matching rows to RHS
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newKeys = i(10);
            TstUtils.addToTable(rhsSource, newKeys, stringCol("J", "anything"));
            rhsSource.notifyListeners(newKeys, RowSetFactory.empty(), RowSetFactory.empty());
        });

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new String[] {"asdf", "asdf"}, ColumnVectors.ofObject(result, "J", String.class).toArray());

        // Remove all rows from RHS
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removedKeys = i(10);
            TstUtils.removeRows(rhsSource, removedKeys);
            rhsSource.notifyListeners(RowSetFactory.empty(), removedKeys, RowSetFactory.empty());
        });

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new String[] {null, null}, ColumnVectors.ofObject(result, "J", String.class).toArray());
    }

    @Test
    public void testStaticEmptyRight() {
        final Table lhs = TableTools.emptyTable(5).update("I=i");
        final Table rhs = TableTools.emptyTable(0).update("J=i", "K=`asdf`");

        Table result = OuterJoinTools.leftOuterJoin(lhs, rhs, "I=J");
        Table expected = lhs.update("J=(int)(null)", "K=(String)(null)");
        assertTableEquals(expected, result);

        result = OuterJoinTools.fullOuterJoin(lhs, rhs, "I=J");
        expected = lhs.update("J=(int)(null)", "K=(String)(null)");
        assertTableEquals(expected, result);
    }

    @Test
    public void testDynamicRightLeftOuterJoin() {
        final Table lhs = testRefreshingTable(intCol("I", 0, 1));
        final QueryTable rhsSource = testRefreshingTable(shortCol("J"));

        // This update creates a SingleValueColumnSource for the RHS table, which causes problems if we query the
        // column source for a value that doesn't exist in the table.
        final Table rhs = rhsSource.update("J=(int)1", "K=`asdf`");

        final Table result = OuterJoinTools.leftOuterJoin(lhs, rhs, "I=J");

        assertEquals(3, result.numColumns());
        assertEquals("I", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("J", result.getDefinition().getColumnsArray()[1].getName());
        assertEquals("K", result.getDefinition().getColumnsArray()[2].getName());

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new int[] {NULL_INT, NULL_INT}, ColumnVectors.ofInt(result, "J").toArray());
        assertArrayEquals(new String[] {null, null}, ColumnVectors.ofObject(result, "K", String.class).toArray());

        ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Add some rows to RHS
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newKeys = i(10, 11);
            TstUtils.addToTable(rhsSource, newKeys, shortCol("J", (short) 100, (short) 200));
            rhsSource.notifyListeners(newKeys, RowSetFactory.empty(), RowSetFactory.empty());
        });

        assertEquals(3, result.size());
        assertArrayEquals(new int[] {0, 1, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new int[] {NULL_INT, 1, 1}, ColumnVectors.ofInt(result, "J").toArray());
        assertArrayEquals(new String[] {null, "asdf", "asdf"},
                ColumnVectors.ofObject(result, "K", String.class).toArray());

        // Remove all rows from RHS
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removedKeys = i(10, 11);
            TstUtils.removeRows(rhsSource, removedKeys);
            rhsSource.notifyListeners(RowSetFactory.empty(), removedKeys, RowSetFactory.empty());
        });

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new int[] {NULL_INT, NULL_INT}, ColumnVectors.ofInt(result, "J").toArray());
        assertArrayEquals(new String[] {null, null}, ColumnVectors.ofObject(result, "K", String.class).toArray());
    }

    @Test
    public void testDynamicRightFullOuterJoin() {
        final Table lhs = testRefreshingTable(intCol("I", 0, 1));
        final QueryTable rhsSource = testRefreshingTable(shortCol("J"));

        // This update creates a SingleValueColumnSource for the RHS table, which causes problems if we query the
        // column source for a value that doesn't exist in the table.
        final Table rhs = rhsSource.update("J=(int)1", "K=`asdf`");

        final Table result = OuterJoinTools.fullOuterJoin(lhs, rhs, "I=J");

        assertEquals(3, result.numColumns());
        assertEquals("I", result.getDefinition().getColumnsArray()[0].getName());
        assertEquals("J", result.getDefinition().getColumnsArray()[1].getName());
        assertEquals("K", result.getDefinition().getColumnsArray()[2].getName());

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new int[] {NULL_INT, NULL_INT}, ColumnVectors.ofInt(result, "J").toArray());
        assertArrayEquals(new String[] {null, null}, ColumnVectors.ofObject(result, "K", String.class).toArray());

        ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Add some rows to RHS
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newKeys = i(10, 11);
            TstUtils.addToTable(rhsSource, newKeys, shortCol("J", (short) 100, (short) 200));
            rhsSource.notifyListeners(newKeys, RowSetFactory.empty(), RowSetFactory.empty());
        });

        assertEquals(3, result.size());
        assertArrayEquals(new int[] {0, 1, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new int[] {NULL_INT, 1, 1}, ColumnVectors.ofInt(result, "J").toArray());
        assertArrayEquals(new String[] {null, "asdf", "asdf"},
                ColumnVectors.ofObject(result, "K", String.class).toArray());

        // Remove all rows from RHS
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removedKeys = i(10, 11);
            TstUtils.removeRows(rhsSource, removedKeys);
            rhsSource.notifyListeners(RowSetFactory.empty(), removedKeys, RowSetFactory.empty());
        });

        assertEquals(2, result.size());
        assertArrayEquals(new int[] {0, 1}, ColumnVectors.ofInt(result, "I").toArray());
        assertArrayEquals(new int[] {NULL_INT, NULL_INT}, ColumnVectors.ofInt(result, "J").toArray());
        assertArrayEquals(new String[] {null, null}, ColumnVectors.ofObject(result, "K", String.class).toArray());
    }
}
