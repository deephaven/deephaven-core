//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.DataAccessHelpers;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.testutil.TstUtils.testTable;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.intCol;
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
        assertEquals(3, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals("Z", DataAccessHelpers.getColumns(result)[2].getName());
        assertEquals(Arrays.asList("a", "b", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 4)));
        assertEquals(Arrays.asList("a", "b", "b", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 4)));
        assertEquals(Arrays.asList(1, 2, 3, null), Arrays.asList(DataAccessHelpers.getColumn(result, "Z").get(0, 4)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b"));
        result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X=Y");
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(2, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", null), Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 3)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("X", "a", "b", "d"));
        result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X");
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(1, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 3)));
    }

    @Test
    public void testLeftOuterJoinStatic() {
        Table lTable = testTable(col("X", "a", "b", "c"));
        Table rTable = testTable(col("Y", "a", "b", "b"), col("Z", 1, 2, 3));
        Table result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X=Y").select();
        assertEquals(4, result.size());
        assertEquals(3, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals("Z", DataAccessHelpers.getColumns(result)[2].getName());
        assertEquals(Arrays.asList("a", "b", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 4)));
        assertEquals(Arrays.asList("a", "b", "b", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 4)));
        assertEquals(Arrays.asList(1, 2, 3, null), Arrays.asList(DataAccessHelpers.getColumn(result, "Z").get(0, 4)));

        lTable = testTable(col("X", "a", "b", "c"));
        rTable = testTable(col("Y", "a", "b"));
        result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X=Y").select();
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(2, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", null), Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 3)));

        lTable = testTable(col("X", "a", "b", "c"));
        rTable = testTable(col("X", "a", "b", "d"));
        result = OuterJoinTools.leftOuterJoin(lTable, rTable, "X").select();
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(1, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 3)));
    }

    @Test
    public void testFullOuterJoin() {
        Table lTable = testRefreshingTable(col("X", "a", "b", "c"));
        Table rTable = testRefreshingTable(col("Y", "a", "b", "b"), intCol("Z", 1, 2, 3));
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        assertEquals(4, result.size());
        assertEquals(3, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals("Z", DataAccessHelpers.getColumns(result)[2].getName());
        assertEquals(Arrays.asList("a", "b", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 4)));
        assertEquals(Arrays.asList("a", "b", "b", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 4)));
        assertEquals(Arrays.asList(1, 2, 3, null), Arrays.asList(DataAccessHelpers.getColumn(result, "Z").get(0, 4)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "d"), intCol("Z", 1, 2, 3));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y", "Y");
        TableTools.showWithRowSet(lTable);
        TableTools.showWithRowSet(rTable);
        TableTools.showWithRowSet(result);
        assertEquals(4, result.size());
        assertEquals(2, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals(Arrays.asList("a", "b", "c", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 4)));
        assertEquals(Arrays.asList("a", "b", null, "d"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 4)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b"));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        TableTools.show(result);
        assertEquals(3, result.size());
        assertEquals(2, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", null), Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 3)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "d"));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        TableTools.show(result);
        assertEquals(4, result.size());
        assertEquals(2, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals(Arrays.asList("a", "b", "c", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 4)));
        assertEquals(Arrays.asList("a", "b", null, "d"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 4)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "b"));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        TableTools.show(result);
        assertEquals(4, result.size());
        assertEquals(2, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals(Arrays.asList("a", "b", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 4)));
        assertEquals(Arrays.asList("a", "b", "b", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 4)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("X", "a", "b", "d"));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X");
        TableTools.show(result);
        assertEquals(4, result.size());
        assertEquals(1, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals(Arrays.asList("a", "b", "c", "d"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 4)));
    }

    @Test
    public void testFullOuterJoinAddColumnsDoesNotIncludeRightMatchColumns() {
        Table lTable = testRefreshingTable(col("X", "a", "b", "c"));
        Table rTable = testRefreshingTable(col("Y", "a", "b", "d"), intCol("Z", 1, 2, 3));
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y", "A=Y");
        assertEquals(4, result.size());
        assertEquals(2, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("A", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals(Arrays.asList("a", "b", "c", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 4)));
        assertEquals(Arrays.asList("a", "b", null, "d"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "A").get(0, 4)));
    }

    @Test
    public void testFullOuterJoinAddColumnRenamesToOverrideMatchColumnName() {
        Table lTable = testRefreshingTable(col("X", "a", "b", "c"));
        Table rTable = testRefreshingTable(col("Y", "a", "b", "d"), col("Z", "e", "f", "g"));
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y", "Y=Z");
        assertEquals(4, result.size());
        assertEquals(2, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals(Arrays.asList("a", "b", "c", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 4)));
        assertEquals(Arrays.asList("e", "f", null, "g"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 4)));
    }

    @Test
    public void testFullOuterJoinMultipleMatchesBothSides() {
        Table lTable = testRefreshingTable(col("X", "a", "a", "b", "c"));
        Table rTable = testRefreshingTable(
                col("Y", "a", "b", "b", "d"),
                intCol("Z", 1, 2, 3, 4));
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y");
        assertEquals(6, result.size());
        assertEquals(3, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals("Z", DataAccessHelpers.getColumns(result)[2].getName());
        assertEquals(Arrays.asList("a", "a", "b", "b", "c", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 6)));
        assertEquals(Arrays.asList("a", "a", "b", "b", null, "d"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 6)));
        assertEquals(Arrays.asList(1, 1, 2, 3, null, 4),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Z").get(0, 6)));

        lTable = testRefreshingTable(col("X", "a", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "b", "d"), intCol("Z", 1, 2, 3, 4));
        result = OuterJoinTools.fullOuterJoin(lTable, rTable, "X=Y", "Y");
        TableTools.showWithRowSet(lTable);
        TableTools.showWithRowSet(rTable);
        TableTools.showWithRowSet(result);
        assertEquals(6, result.size());
        assertEquals(2, DataAccessHelpers.getColumns(result).length);
        assertEquals("X", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("Y", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals(Arrays.asList("a", "a", "b", "b", "c", null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "X").get(0, 6)));
        assertEquals(Arrays.asList("a", "a", "b", "b", null, "d"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Y").get(0, 6)));
    }

    @Test
    public void testFullOuterJoinIdentityMatchWithAddColumn() {
        Table lTable = TableTools.emptyTable(4).update("a = i", "b = i * 2");
        Table rTable = TableTools.emptyTable(4).update("a = i", "c = i * 3");
        Table result = OuterJoinTools.fullOuterJoin(lTable, rTable, "a", "c");

        assertEquals(4, result.size());
        assertEquals(3, DataAccessHelpers.getColumns(result).length);
        assertEquals("a", DataAccessHelpers.getColumns(result)[0].getName());
        assertEquals("b", DataAccessHelpers.getColumns(result)[1].getName());
        assertEquals("c", DataAccessHelpers.getColumns(result)[2].getName());
        assertEquals(Arrays.asList(0, 1, 2, 3), Arrays.asList(DataAccessHelpers.getColumn(result, "a").get(0, 9)));
        assertEquals(Arrays.asList(0, 2, 4, 6), Arrays.asList(DataAccessHelpers.getColumn(result, "b").get(0, 9)));
        assertEquals(Arrays.asList(0, 3, 6, 9), Arrays.asList(DataAccessHelpers.getColumn(result, "c").get(0, 9)));
    }
}
