//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.RangeFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.TimeSeriesFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterImpl;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                viewColumns,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

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
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

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
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                whereFilters);

        Assert.eqTrue(deferredTable.isRefreshing(), "deferredTable.isRefreshing()");
    }

    @Test
    public void testUpdateColumnToDifferentType() {
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofLong("X"));
        final Table sourceTable = TableTools.emptyTable(10).update("X = ii");

        final SelectColumn[] viewColumns = SelectColumn.from(
                Selectable.parse("X = (int)X"));

        final DeferredViewTable deferredTable = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                viewColumns,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

        final Class<?> resultType = deferredTable.coalesce().getDefinition().getColumn("X").getDataType();
        Assert.eq(resultType, "resultType", int.class, "int.class");
    }

    @Test
    public void testNoFiltersJumpOverSerial() {
        // assert that filters cannot be bumped past a serial column
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofLong("X"));
        final Table sourceTable = TableTools.emptyTable(100_000).update("X = ii");

        final SizeRecorderFilter serialFilter =
                new SizeRecorderFilter(new RangeFilter("Y", Condition.LESS_THAN, "50000").withSerial());
        final SizeRecorderFilter freeFilter =
                new SizeRecorderFilter(new RangeFilter("X", Condition.LESS_THAN, "25000"));

        Table deferredTable = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY)
                .updateView("Y = ii")
                .where(ConjunctiveFilter.of(serialFilter, freeFilter))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 25000);
        Assert.eq(serialFilter.numRowsFiltered(), "serialFilter.numRowsFiltered()", 100_000);
        Assert.eq(freeFilter.numRowsFiltered(), "freeFilter.numRowsFiltered()", 50_000);

        // ensure that it does jump over it if it is not serial
        final SizeRecorderFilter notSerialFilter =
                new SizeRecorderFilter(new RangeFilter("Y", Condition.LESS_THAN, "50000"));
        freeFilter.reset();

        deferredTable = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY)
                .updateView("Y = ii")
                .where(ConjunctiveFilter.of(notSerialFilter, freeFilter))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 25000);
        Assert.eq(notSerialFilter.numRowsFiltered(), "notSerialFilter.numRowsFiltered()", 25_000);
        Assert.eq(freeFilter.numRowsFiltered(), "freeFilter.numRowsFiltered()", 100_000);
    }

    // TODO NOCOMMIT NATE filters should validate renaming behavior
    // TODO NOCOMMIT NATE test rename inside of a barrier / respectsBarrier / serial wrapper

    @Test
    public void testBarrierWithNewlyDefinedColumn() {
        // assert that filters with barriers can jump over other filters
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofLong("X"));
        final Table sourceTable = TableTools.emptyTable(100_000).update("X = ii");

        final Object barrier = new Object();
        final SizeRecorderFilter filter0 =
                new SizeRecorderFilter(new RangeFilter("X", Condition.LESS_THAN, "50000"));
        final SizeRecorderFilter filter1 =
                new SizeRecorderFilter(new RangeFilter("X", Condition.LESS_THAN, "25000"));
        final SizeRecorderFilter filter2 =
                new SizeRecorderFilter(new RangeFilter("Y", Condition.LESS_THAN, "10000"));

        // filter0 and filter1 should both jump over filter2
        final Table deferredTable = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY)
                .updateView("Y = ii")
                .where(ConjunctiveFilter.of(
                        filter2,
                        filter0.withBarrier(barrier),
                        filter1.respectsBarrier(barrier)))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 10_000);
        Assert.eq(filter0.numRowsFiltered(), "filter0.numRowsFiltered()", 100_000);
        Assert.eq(filter1.numRowsFiltered(), "filter1.numRowsFiltered()", 50_000);
        Assert.eq(filter2.numRowsFiltered(), "filter2.numRowsFiltered()", 25_000);
    }

    @Test
    public void testRespectsBarrierNoJump() {
        // assert respectsBarrier that cant jump still works
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofLong("X"));
        final Table sourceTable = TableTools.emptyTable(100_000).update("X = ii");

        final Object barrier = new Object();
        final SizeRecorderFilter filter0 =
                new SizeRecorderFilter(new RangeFilter("Y", Condition.LESS_THAN, "5000"));
        final SizeRecorderFilter filter1 =
                new SizeRecorderFilter(new RangeFilter("X", Condition.LESS_THAN, "25000"));
        final SizeRecorderFilter filter2 =
                new SizeRecorderFilter(new RangeFilter("X", Condition.LESS_THAN, "10000"));

        // filter2 jumps over filter0 and filter1
        final Table deferredTable = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY)
                .updateView("Y = ii")
                .where(ConjunctiveFilter.of(
                        filter0.withBarrier(barrier),
                        filter1.respectsBarrier(barrier),
                        filter2))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 5_000);
        Assert.eq(filter0.numRowsFiltered(), "filter0.numRowsFiltered()", 10_000);
        Assert.eq(filter1.numRowsFiltered(), "filter1.numRowsFiltered()", 5_000);
        Assert.eq(filter2.numRowsFiltered(), "filter2.numRowsFiltered()", 100_000);
    }

    @Test
    public void testBarrierSplitByView() {
        // assert respectsBarrier that cant jump still works
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofLong("X"));
        final Table sourceTable = TableTools.emptyTable(100_000).update("X = ii");

        final Object barrier = new Object();
        final SizeRecorderFilter filter0 =
                new SizeRecorderFilter(new RangeFilter("X", Condition.LESS_THAN, "50000"));
        final SizeRecorderFilter filter1 =
                new SizeRecorderFilter(new RangeFilter("Y", Condition.LESS_THAN, "25000"));

        // filter0 can move over, but filter1 is blocked by a new column
        Table deferredTable = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY)
                .updateView("Y = ii")
                .where(ConjunctiveFilter.of(
                        filter0.withBarrier(barrier),
                        filter1.respectsBarrier(barrier)))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 25_000);
        Assert.eq(filter0.numRowsFiltered(), "filter0.numRowsFiltered()", 100_000);
        Assert.eq(filter1.numRowsFiltered(), "filter1.numRowsFiltered()", 50_000);
    }

    protected static class SizeRecorderFilter extends WhereFilterImpl {
        private final TLongList sizes = new TLongArrayList();
        private final WhereFilter innerFilter;

        SizeRecorderFilter() {
            this(null);
        }

        SizeRecorderFilter(final Filter filter) {
            this.innerFilter = filter == null ? null : WhereFilter.of(filter);
        }

        @Override
        public List<String> getColumns() {
            return innerFilter == null ? Collections.emptyList() : innerFilter.getColumns();
        }

        @Override
        public List<String> getColumnArrays() {
            return innerFilter == null ? Collections.emptyList() : innerFilter.getColumnArrays();
        }

        @Override
        public void init(@NotNull final TableDefinition tableDefinition) {
            if (innerFilter != null) {
                innerFilter.init(tableDefinition);
            }
        }

        @Override
        public void init(@NotNull TableDefinition tableDefinition,
                @NotNull QueryCompilerRequestProcessor compilationProcessor) {
            if (innerFilter != null) {
                innerFilter.init(tableDefinition, compilationProcessor);
            }
        }

        @NotNull
        @Override
        public WritableRowSet filter(
                @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
            synchronized (sizes) {
                sizes.add(selection.size());
            }
            if (innerFilter != null) {
                return innerFilter.filter(selection, fullSet, table, usePrev);
            }
            return selection.copy();
        }

        @Override
        public boolean isSimpleFilter() {
            return innerFilter == null || innerFilter.isSimpleFilter();
        }

        @Override
        public boolean permitParallelization() {
            return innerFilter == null || innerFilter.permitParallelization();
        }

        @Override
        public void setRecomputeListener(RecomputeListener result) {
            if (innerFilter != null) {
                innerFilter.setRecomputeListener(result);
            }
        }

        @Override
        public WhereFilter copy() {
            return this;
        }

        protected void reset() {
            sizes.clear();
        }

        protected long numRowsFiltered() {
            synchronized (sizes) {
                return sizes.sum();
            }
        }
    }
}
