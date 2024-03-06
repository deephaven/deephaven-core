//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;

import java.util.stream.Collectors;

public class TestMoveColumns extends RefreshingTableTestCase {

    private Table table;
    private int numCols;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        table = TableTools.emptyTable(1).update("a=1", "b=2", "c=3", "d=4", "e=5");
        numCols = table.numColumns();
    }

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

        temp = table.moveColumns(0, "x=a", "d");
        checkColumnOrder(temp, "xdbce");
        checkColumnValueOrder(temp, "14235");

        temp = table.moveColumns(0, "b=a", "a=b");
        checkColumnOrder(temp, "bacde");
        checkColumnValueOrder(temp, "12345");
    }

    public void testMoveUpColumns() {
        // basic moving
        checkColumnOrder(table.moveColumnsUp("a"), "abcde");

        checkColumnOrder(table.moveColumnsUp("b"), "bacde");

        checkColumnOrder(table.moveColumnsUp("b", "c", "d", "e"), "bcdea");

        // moving and renaming
        Table temp = table.moveColumnsUp("x=a");
        checkColumnOrder(temp, "xbcde");
        checkColumnValueOrder(temp, "12345");

        temp = table.moveColumnsUp("x=e");
        checkColumnOrder(temp, "xabcd");
        checkColumnValueOrder(temp, "51234");
    }

    public void testMoveDownColumns() {
        checkColumnOrder(table.moveColumnsDown("a"), "bcdea");

        checkColumnOrder(table.moveColumnsDown("b"), "acdeb");

        checkColumnOrder(table.moveColumnsDown("a", "b", "c", "d"), "eabcd");

        // moving and renaming
        Table temp = table.moveColumnsDown("x=a");
        checkColumnOrder(temp, "bcdex");
        checkColumnValueOrder(temp, "23451");

        temp = table.moveColumnsDown("b=a", "a=b", "c");
        checkColumnOrder(temp, "debac");
        checkColumnValueOrder(temp, "45123");

        temp = table.moveColumnsDown("b=a", "a=b", "c");
        checkColumnOrder(temp, "debac");
        checkColumnValueOrder(temp, "45123");
    }

    private void checkColumnOrder(Table t, String expectedOrder) {
        final String order = t.getDefinition()
                .getColumnStream()
                .map(ColumnDefinition::getName)
                .collect(Collectors.joining(""));
        assertEquals(expectedOrder, order);
    }

    private void checkColumnValueOrder(Table t, String expectedOrder) {
        final String order = t.getColumnSources().stream().mapToInt((col) -> col.getInt(0))
                .mapToObj(String::valueOf).collect(Collectors.joining(""));
        assertEquals(expectedOrder, order);
    }

}
