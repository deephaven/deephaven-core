/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.Filters;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.db.plot.BaseFigureImpl;
import io.deephaven.db.plot.filters.SelectableDataSet;
import io.deephaven.db.plot.filters.SelectableDataSetOneClick;
import io.deephaven.db.plot.filters.Selectables;
import io.deephaven.db.plot.util.tables.SwappableTable;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.TableMap;

import java.util.*;

public class TestSelectables extends BaseArrayTestCase {
    private final String[] categories = {"A", "B", "C"};
    private final double[] values = {1, 2, 3};
    private final String byColumn = "Cats";
    private final String valueColumn = "Values";
    private final Table table =
            TableTools.newTable(TableTools.col(byColumn, categories), TableTools.doubleCol(valueColumn, values));
    private final BaseFigureImpl figure = new BaseFigureImpl();

    public void testFilteredTableOneClick() {
        try {
            Selectables.oneClick((Table) null, byColumn);
            fail("Expected an exception");
        } catch (RequirementFailure e) {
            assertTrue(e.getMessage().contains("null"));
        }
        try {
            Selectables.oneClick((Table) null);
            fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("empty"));
        }
        testFilteredTable(Selectables.oneClick(table, byColumn));
        testFilteredTable(new SelectableDataSetOneClick(table.byExternal(byColumn), table.getDefinition(),
                new String[] {byColumn}));
    }

    private void testFilteredTable(SelectableDataSet<String, Set<Object>> selectableDataSet) {
        SwappableTable swappableTable =
                selectableDataSet.getSwappableTable("M", figure.newChart(), t -> t, byColumn, valueColumn);
        testTableMapEquals(table.byExternal(byColumn), swappableTable.getTableMapHandle().getTableMap());
    }

    private void testTableMapEquals(final TableMap t1, final TableMap t2) {
        for (final String c : categories) {
            testTableEquals(t1.get(c), t2.get(c));
        }
    }

    private void testTableEquals(final Table t1, final Table t2) {
        final List<String> columnNames = new ArrayList<>();
        Arrays.stream(t1.getColumns()).forEach(col -> columnNames.add(col.getName()));
        assertEquals(columnNames.size(), t2.getColumns().length);
        t2.hasColumns(columnNames);
    }
}
