//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.Filters;

import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.plot.filters.SelectableDataSetOneClick;
import io.deephaven.plot.filters.Selectables;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.PartitionedTable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class TestSelectables {

    @Rule
    final public EngineCleanup framework = new EngineCleanup();

    private final String[] categories = {"A", "B", "C"};
    private final double[] values = {1, 2, 3};
    private final String byColumn = "Cats";
    private final String valueColumn = "Values";
    private final BaseFigureImpl figure = new BaseFigureImpl();

    private Table table;

    @Before
    public void setUp() {
        table = TableTools.newTable(TableTools.col(byColumn, categories), TableTools.doubleCol(valueColumn, values));
    }

    @Test
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
        testFilteredTable(new SelectableDataSetOneClick(table.partitionBy(byColumn)));
    }

    private void testFilteredTable(SelectableDataSet<String, Set<Object>> selectableDataSet) {
        SwappableTable swappableTable =
                selectableDataSet.getSwappableTable("M", figure.newChart(), t -> t, byColumn, valueColumn);
        testPartitionedTableEquals(table.partitionBy(byColumn),
                swappableTable.getPartitionedTableHandle().getPartitionedTable());
    }

    private void testPartitionedTableEquals(final PartitionedTable t1, final PartitionedTable t2) {
        for (final String c : categories) {
            testTableEquals(t1.constituentFor(c), t2.constituentFor(c));
        }
    }

    private void testTableEquals(final Table t1, final Table t2) {
        assertNotNull(t1);
        assertNotNull(t2);
        final List<String> columnNames = t1.getDefinition().getColumnNames();
        assertEquals(columnNames.size(), t2.numColumns());
        t2.hasColumns(columnNames);
    }
}
