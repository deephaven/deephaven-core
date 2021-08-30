package io.deephaven.db.tables;

import io.deephaven.db.tables.utils.TableTools;
import junit.framework.TestCase;

import java.util.stream.Collectors;

public class TestMoveColumns extends TestCase {
    private static final Table table = TableTools.emptyTable(1).update("a=1", "b=2", "c=3", "d=4", "e=5");
    private static final int numCols = table.getColumns().length;

    public void testMoveColumns() {
        // Basic moving
        Table temp = table.moveColumns(0, "a");
        checkColumnOrder(temp, "abcde");
        checkColumnValueOrder(temp, "12345");

        temp = table.moveColumns(numCols - 1, "a");
        checkColumnOrder(temp, "bcdea");
        checkColumnValueOrder(temp, "23451");

        temp = table.moveColumns(0, "a", "b");
        checkColumnOrder(temp, "abcde");
        checkColumnValueOrder(temp, "12345");

        temp = table.moveColumns(numCols - 2, "a", "b");
        checkColumnOrder(temp, "cdeab");
        checkColumnValueOrder(temp, "34512");

        // Basic moving with renaming
        temp = table.moveColumns(0, "x=a");
        checkColumnOrder(temp, "xbcde");
        checkColumnValueOrder(temp, "12345");

        temp = table.moveColumns(numCols - 1, "x=a");
        checkColumnOrder(temp, "bcdex");
        checkColumnValueOrder(temp, "23451");

        temp = table.moveColumns(0, "x=a", "y=b");
        checkColumnOrder(temp, "xycde");
        checkColumnValueOrder(temp, "12345");

        temp = table.moveColumns(0, "b=a");
        checkColumnOrder(temp, "bcde");
        checkColumnValueOrder(temp, "1345");

        temp = table.moveColumns(0, "x=a", "a=b");
        checkColumnOrder(temp, "xacde");
        checkColumnValueOrder(temp, "12345");

        temp = table.moveColumns(0, "x=a", "a=b", "b=c");
        checkColumnOrder(temp, "xabde");
        checkColumnValueOrder(temp, "12345");

        temp = table.moveColumns(numCols - 3, "x=a", "a=b", "b=c");
        checkColumnOrder(temp, "dexab");
        checkColumnValueOrder(temp, "45123");

        temp = table.moveColumns(0, "x=a", "b=x");
        checkColumnOrder(temp, "xbcde");
        checkColumnValueOrder(temp, "11345");

        temp = table.moveColumns(0, "x=a", "y=a", "z=a");
        checkColumnOrder(temp, "xyzbcde");
        checkColumnValueOrder(temp, "1112345");

        temp = table.moveColumns(0, "b=a", "a=b");
        checkColumnOrder(temp, "bacde");
        checkColumnValueOrder(temp, "11345");

        temp = table.moveColumns(0, "d=c", "d=a", "x=e");
        checkColumnOrder(temp, "dxb");
        checkColumnValueOrder(temp, "152");

        temp = table.moveColumns(0, "a=b", "a=c");
        checkColumnOrder(temp, "ade");
        checkColumnValueOrder(temp, "345");

        temp = table.moveColumns(0, "a=b", "a=c", "a=d", "a=e");
        checkColumnOrder(temp, "a");
        checkColumnValueOrder(temp, "5");
    }

    public void testMoveUpColumns() {
        // basic moving
        checkColumnOrder(table.moveUpColumns("a"), "abcde");

        checkColumnOrder(table.moveUpColumns("b"), "bacde");

        checkColumnOrder(table.moveUpColumns("b", "c", "d", "e"), "bcdea");

        // moving and renaming
        Table temp = table.moveUpColumns("x=a");
        checkColumnOrder(temp, "xbcde");
        checkColumnValueOrder(temp, "12345");

        temp = table.moveUpColumns("x=e");
        checkColumnOrder(temp, "xabcd");
        checkColumnValueOrder(temp, "51234");

        temp = table.moveUpColumns("x=a", "x=b");
        checkColumnOrder(temp, "xcde");
        checkColumnValueOrder(temp, "2345");

        temp = table.moveUpColumns("x=a", "y=a");
        checkColumnOrder(temp, "xybcde");
        checkColumnValueOrder(temp, "112345");
    }

    public void testMoveDownColumns() {
        checkColumnOrder(table.moveDownColumns("a"), "bcdea");

        checkColumnOrder(table.moveDownColumns("b"), "acdeb");

        checkColumnOrder(table.moveDownColumns("a", "b", "c", "d"), "eabcd");

        // moving and renaming
        Table temp = table.moveDownColumns("x=a");
        checkColumnOrder(temp, "bcdex");
        checkColumnValueOrder(temp, "23451");

        temp = table.moveDownColumns("b=a", "a=b", "c");
        checkColumnOrder(temp, "debac");
        checkColumnValueOrder(temp, "45113");

        temp = table.moveDownColumns("b=a", "a=b", "c", "d=a");
        checkColumnOrder(temp, "ebacd");
        checkColumnValueOrder(temp, "51131");

        temp = table.moveDownColumns("x=a", "x=b");
        checkColumnOrder(temp, "cdex");
        checkColumnValueOrder(temp, "3452");

        temp = table.moveDownColumns("x=a", "y=a");
        checkColumnOrder(temp, "bcdexy");
        checkColumnValueOrder(temp, "234511");
    }

    private void checkColumnOrder(Table t, String expectedOrder) {
        final String order = t.getColumnSourceMap().keySet().stream().collect(Collectors.joining(""));
        assertEquals(expectedOrder, order);
    }

    private void checkColumnValueOrder(Table t, String expectedOrder) {
        final String order = t.getColumnSourceMap().values().stream().mapToInt((col) -> col.getInt(0))
                .mapToObj(String::valueOf).collect(Collectors.joining(""));
        assertEquals(expectedOrder, order);
    }

}
