//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.RangeFilter;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.type.ArrayTypeUtils;
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

        // We'll use a incremental release filter for convenience but any refreshing filter will do.
        final WhereFilter[] whereFilters = new WhereFilter[] {
                new IncrementalReleaseFilter(1, 1)
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

        final RowSetCapturingFilter serialFilter =
                new RowSetCapturingFilter(new RangeFilter("Y", Condition.LESS_THAN, "50000").withSerial());
        final RowSetCapturingFilter freeFilter =
                new RowSetCapturingFilter(new RangeFilter("X", Condition.LESS_THAN, "25000"));

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
        Assert.eq(numRowsFiltered(serialFilter), "numRowsFiltered(serialFilter)", 100_000);
        Assert.eq(numRowsFiltered(freeFilter), "numRowsFiltered(freeFilter)", 50_000);

        // ensure that it does jump over it if it is not serial
        final RowSetCapturingFilter notSerialFilter =
                new RowSetCapturingFilter(new RangeFilter("Y", Condition.LESS_THAN, "50000"));
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
        Assert.eq(numRowsFiltered(notSerialFilter), "numRowsFiltered(notSerialFilter)", 25_000);
        Assert.eq(numRowsFiltered(freeFilter), "numRowsFiltered(freeFilter)", 100_000);
    }

    @Test
    public void testBarrierWithNewlyDefinedColumn() {
        // assert that filters with barriers can jump over other filters
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofLong("X"));
        final Table sourceTable = TableTools.emptyTable(100_000).update("X = ii");

        final Object barrier = new Object();
        final RowSetCapturingFilter filter0 =
                new RowSetCapturingFilter(new RangeFilter("X", Condition.LESS_THAN, "50000"));
        final RowSetCapturingFilter filter1 =
                new RowSetCapturingFilter(new RangeFilter("X", Condition.LESS_THAN, "25000"));
        final RowSetCapturingFilter filter2 =
                new RowSetCapturingFilter(new RangeFilter("Y", Condition.LESS_THAN, "10000"));

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
                        filter0.withBarriers(barrier),
                        filter1.respectsBarriers(barrier)))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 10_000);
        Assert.eq(numRowsFiltered(filter0), "filter0.numRowsFiltered(filter0)", 100_000);
        Assert.eq(numRowsFiltered(filter1), "filter1.numRowsFiltered(filter1)", 50_000);
        Assert.eq(numRowsFiltered(filter2), "filter2.numRowsFiltered(filter2)", 25_000);
    }

    @Test
    public void testRespectsBarrierNoJump() {
        // assert barrier + respectsBarrier that cant jump still works
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofLong("X"));
        final Table sourceTable = TableTools.emptyTable(100_000).update("X = ii");

        final Object barrier = new Object();
        final RowSetCapturingFilter filter0 =
                new RowSetCapturingFilter(new RangeFilter("Y", Condition.LESS_THAN, "5000"));
        final RowSetCapturingFilter filter1 =
                new RowSetCapturingFilter(new RangeFilter("X", Condition.LESS_THAN, "25000"));
        final RowSetCapturingFilter filter2 =
                new RowSetCapturingFilter(new RangeFilter("X", Condition.LESS_THAN, "10000"));

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
                        filter0.withBarriers(barrier),
                        filter1.respectsBarriers(barrier),
                        filter2))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 5_000);
        Assert.eq(numRowsFiltered(filter0), "numRowsFiltered(filter0)", 10_000);
        Assert.eq(numRowsFiltered(filter1), "numRowsFiltered(filter1)", 5_000);
        Assert.eq(numRowsFiltered(filter2), "numRowsFiltered(filter2)", 100_000);
    }

    @Test
    public void testBarrierSplitByView() {
        // assert respectsBarrier that cant jump with the barrier still works
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofLong("X"));
        final Table sourceTable = TableTools.emptyTable(100_000).update("X = ii");

        final Object barrier = new Object();
        final RowSetCapturingFilter filter0 =
                new RowSetCapturingFilter(new RangeFilter("X", Condition.LESS_THAN, "50000"));
        final RowSetCapturingFilter filter1 =
                new RowSetCapturingFilter(new RangeFilter("Y", Condition.LESS_THAN, "25000"));

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
                        filter0.withBarriers(barrier),
                        filter1.respectsBarriers(barrier)))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 25_000);
        Assert.eq(numRowsFiltered(filter0), "numRowsFiltered(filter0)", 100_000);
        Assert.eq(numRowsFiltered(filter1), "numRowsFiltered(filter1)", 50_000);
    }

    @Test
    public void testMatchFilterRename() {
        verifyFilterIsPrioritized(new MatchFilter(MatchFilter.MatchType.Regular, "Y", "A"));
    }

    @Test
    public void testConditionFilterRename() {
        verifyFilterIsPrioritized(ConditionFilter.createConditionFilter("Y = `A`"));
    }

    @Test
    public void testInvertedWrappedRenames() {
        final WhereFilter filter = new MatchFilter(MatchFilter.MatchType.Regular, "Y", "B", "C", "D");
        verifyFilterIsPrioritized(WhereFilterInvertedImpl.of(filter));
    }

    @Test
    public void testSerialWrappedRenames() {
        // note serial filters require incoming rowsets to match as if all previous filters were applied
        final Filter filter = new MatchFilter(MatchFilter.MatchType.Regular, "Y", "A");
        verifyFilterIsPrioritized(filter.withSerial(), false);
    }

    @Test
    public void testBarrierWrappedRenames() {
        final Filter filter = new MatchFilter(MatchFilter.MatchType.Regular, "Y", "A");
        verifyFilterIsPrioritized(filter.withBarriers(new Object()));
    }

    @Test
    public void testRespectsBarrierWrappedRenames() {
        final Object barrier = new Object();
        final WhereFilter filter = new MatchFilter(MatchFilter.MatchType.Regular, "Y", "A");
        verifyFilterIsPrioritized(ConjunctiveFilter.of(
                ConditionFilter.createConditionFilter("true").withBarriers(barrier),
                filter.respectsBarriers(barrier)));
    }

    @Test
    public void testConjunctiveFilterRename() {
        // get crafty nesting a conjunctive filter inside of a disjunctive filter
        final WhereFilter filter = new MatchFilter(MatchFilter.MatchType.Regular, "Y", "A");
        verifyFilterIsPrioritized(DisjunctiveFilter.of(
                ConditionFilter.createConditionFilter("false"),
                ConjunctiveFilter.of(
                        ConditionFilter.createConditionFilter("true"),
                        filter.withBarriers(new Object()))));
    }


    @Test
    public void testNestedConjunctiveFilterRename() {
        // get crafty with some nesting
        final WhereFilter filter = new MatchFilter(MatchFilter.MatchType.Regular, "Y", "A");
        verifyFilterIsPrioritized(ConjunctiveFilter.of(
                ConditionFilter.createConditionFilter("true"),
                ConjunctiveFilter.of(
                        ConditionFilter.createConditionFilter("true"),
                        filter.withBarriers(new Object()))));
    }


    @Test
    public void testNestedDisjunctiveMultiFilterRename() {
        // goal of this test is to rename multiple inner filters via a top-level filter
        final WhereFilter filter0 = new MatchFilter(MatchFilter.MatchType.Regular, "Y", "A");
        final WhereFilter filter1 = new MatchFilter(MatchFilter.MatchType.Regular, "Y2", "A");
        verifyFilterIsPrioritized(DisjunctiveFilter.of(filter0, filter1));
    }

    private void verifyFilterIsPrioritized(final Filter filterToTest) {
        verifyFilterIsPrioritized(filterToTest, true);
    }

    private void verifyFilterIsPrioritized(final Filter filterToTest, final boolean expectsJump) {
        final String[] values = new String[] {"A", "B", "C", "D"};
        ExecutionContext.getContext().getQueryScope().putParam("values", values);

        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofString("X"),
                ColumnDefinition.ofLong("I"));
        final Table sourceTable = TableTools.emptyTable(100_000)
                .update("X = values[i % 4]", "I = ii");

        final RowSetCapturingFilter filter1 =
                new RowSetCapturingFilter(new RangeFilter("I", Condition.LESS_THAN, "25000"));

        // here we expect that filter0 can jump over filter1 w/a rename
        final Table source = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY)
                .updateView("Y = X", "Y2 = Y", "I = I + 0");
        final Table deferredTable = source
                .where(ConjunctiveFilter.of(
                        filter1,
                        WhereFilter.of(filterToTest)))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 6250);
        if (expectsJump) {
            final Table expected = source.where(filterToTest);
            Assert.eq(numRowsFiltered(filter1), "numRowsFiltered(filter1)", expected.size(), "expected.size()");
        } else {
            Assert.eq(numRowsFiltered(filter1), "numRowsFiltered(filter1)", 100_000);
        }
    }

    private static long numRowsFiltered(final RowSetCapturingFilter filter) {
        return filter.rowSets().stream()
                .mapToLong(RowSet::size)
                .sum();
    }
}
