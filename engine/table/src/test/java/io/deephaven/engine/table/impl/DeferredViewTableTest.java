package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.TimeSeriesFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;

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
        final Table expectedTable = sourceTable.update(Arrays.asList(viewColumns));
        TstUtils.assertTableEquals(expectedTable, resultTable);
    }

    @Test
    public void testIsRefreshingViaSource() {
        testIsRefreshingViaSource(false);
        testIsRefreshingViaSource(true);
    }

    private void testIsRefreshingViaSource(boolean sourceRefreshing) {
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.fromGenericType("X", int.class),
                ColumnDefinition.fromGenericType("Y", int.class));
        final Table sourceTable = TableTools.emptyTable(10);
        if (sourceRefreshing) {
            sourceTable.setRefreshing(true);
        }

        final DeferredViewTable deferredTable = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.SimpleTableReference(sourceTable),
                CollectionUtil.ZERO_LENGTH_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY);

        Assert.eq(deferredTable.isRefreshing(), "deferredTable.isRefreshing()", sourceRefreshing, "sourceRefreshing");
    }

    @Test
    public void testIsRefreshingViaFilter() {
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.fromGenericType("Timestamp", Instant.class));
        final Table sourceTable = TableTools.emptyTable(10).update("Timestamp = DateTimeUtils.now()");

        // We'll use a time series filter for convenience but any refreshing filter will do.
        final WhereFilter[] whereFilters = new WhereFilter[] {
                new TimeSeriesFilter("Timestamp", "PT1s")
        };
        Assert.eqTrue(whereFilters[0].isRefreshing(), "whereFilters[0].isRefreshing()");

        final DeferredViewTable deferredTable = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.SimpleTableReference(sourceTable),
                CollectionUtil.ZERO_LENGTH_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                whereFilters);

        Assert.eqTrue(deferredTable.isRefreshing(), "deferredTable.isRefreshing()");
    }
}
