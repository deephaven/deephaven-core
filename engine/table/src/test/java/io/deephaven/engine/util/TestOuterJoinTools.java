//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.testutil.TstUtils.testTable;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.intCol;
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
}
