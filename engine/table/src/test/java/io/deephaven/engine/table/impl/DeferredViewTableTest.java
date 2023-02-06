package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

public class DeferredViewTableTest {
    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    @Test
    public void testDeferredViewTableCanUseIntermediateColumns() {
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.fromGenericType("X", int.class),
                ColumnDefinition.fromGenericType("Y", int.class));
        final Table sourceTable = TableTools.emptyTable(10);
        final SelectColumn[] viewColumns = SelectColumn.from(
                Selectable.parse("X = ii"),
                Selectable.parse("Y = 2 * X"));

        final DeferredViewTable deferredTable = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.SimpleTableReference(sourceTable),
                CollectionUtil.ZERO_LENGTH_STRING_ARRAY,
                viewColumns,
                WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY);

        final Table resultTable = deferredTable.coalesce();
        final Table expectedTable = sourceTable.update(viewColumns);
        TstUtils.assertTableEquals(expectedTable, resultTable);
    }
}
