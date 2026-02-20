//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.RangeFilter;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.type.ArrayTypeUtils;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

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
                new RowSetCapturingFilter(new RangeFilter("Y", Condition.LESS_THAN, "50000"));
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
                .where(ConjunctiveFilter.of(serialFilter.withSerial(), freeFilter))
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
                        filter0.withDeclaredBarriers(barrier),
                        filter1.withRespectedBarriers(barrier)))
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
                        filter0.withDeclaredBarriers(barrier),
                        filter1.withRespectedBarriers(barrier),
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
                        filter0.withDeclaredBarriers(barrier),
                        filter1.withRespectedBarriers(barrier)))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 25_000);
        Assert.eq(numRowsFiltered(filter0), "numRowsFiltered(filter0)", 100_000);
        Assert.eq(numRowsFiltered(filter1), "numRowsFiltered(filter1)", 50_000);
    }

    @Test
    public void testMatchFilterRename() {
        verifyFilterIsPrioritized(new MatchFilter(MatchOptions.REGULAR, "Y", "A"));
    }

    @Test
    public void testMatchFilterDoubleRename() {
        verifyFilterIsPrioritized(new MatchFilter(MatchOptions.REGULAR, "Y", "A"), true,
                DeferredViewTableTest::doubleRenameUpdate);
    }

    @Test
    public void testRangeFilterPostView() {
        verifyFilterIsPrioritized(new LongRangeFilter("I", 0, 6250, true, false), false,
                DeferredViewTableTest::postViewSourceUpdate);
    }

    /**
     * The RangeFilter does not support renames
     */
    @Test
    public void testRangeFilterWithRename() {
        verifyFilterIsPrioritized(new LongRangeFilter("J", 0, 6250, true, false), false,
                DeferredViewTableTest::longRenameUpdate);
    }

    /**
     * The RangeFilter does not support renames
     */
    @Test
    public void testInvertedRangeFilterWithRename() {
        verifyFilterIsPrioritized(new LongRangeFilter("J", 6250, 1_000_0000, true, false).invert(), false,
                DeferredViewTableTest::longRenameUpdate);
    }

    /**
     * The RangeFilter does not support renames
     */
    @Test
    public void testRangeFilterWithDeclaredBarrier() {
        final Object barrier = new Object();
        verifyFilterIsPrioritized(
                ConjunctiveFilter.of(new LongRangeFilter("J", 0, 6250, true, false).withDeclaredBarriers(barrier)),
                false, DeferredViewTableTest::longRenameUpdate);
    }

    /**
     * The RangeFilter does not support renames
     */
    @Test
    public void testRangeFilterWithRespectedBarrier() {
        final Object barrier = new Object();
        verifyFilterIsPrioritized(
                DisjunctiveFilter.of(
                        ConjunctiveFilter.of(
                                ConditionFilter.createConditionFilter("true").withDeclaredBarriers(barrier),
                                new LongRangeFilter("J", 0, 6250, true, false).withRespectedBarriers(barrier)),
                        new LongRangeFilter("J", -2, -1, true, false)),
                false, DeferredViewTableTest::longRenameUpdate);
    }

    /**
     * The RangeFilter does not support renames
     */
    @Test
    public void testDisjunctiveRangeFilter() {
        verifyFilterIsPrioritized(
                DisjunctiveFilter.of(new LongRangeFilter("J", 0, 3000, true, false),
                        new LongRangeFilter("J", 3000, 6250, true, false)),
                false, DeferredViewTableTest::longRenameUpdate);
    }

    /**
     * The RangeFilter does not support renames
     */
    @Test
    public void testConjunctiveRangeFilter() {
        verifyFilterIsPrioritized(DisjunctiveFilter.of(
                ConjunctiveFilter.of(new LongRangeFilter("J", 0, 25000, true, false),
                        new LongRangeFilter("J", 0, 6250, true, false)),
                new LongRangeFilter("J", -2, -1, true, false)), false, DeferredViewTableTest::longRenameUpdate);
    }

    @Test
    public void testConditionFilterRename() {
        verifyFilterIsPrioritized(ConditionFilter.createConditionFilter("Y = `A`"));
    }

    @Test
    public void testInvertedWrappedRenames() {
        final WhereFilter filter = new MatchFilter(MatchOptions.REGULAR, "Y", "B", "C", "D");
        verifyFilterIsPrioritized(WhereFilterInvertedImpl.of(filter));
    }

    @Test
    public void testSerialWrappedRenames() {
        // note serial filters require incoming rowsets to match as if all previous filters were applied
        final Filter filter = new MatchFilter(MatchOptions.REGULAR, "Y", "A");
        verifyFilterIsPrioritized(filter.withSerial(), false, DeferredViewTableTest::simpleUpdate);
    }

    @Test
    public void testBarrierWrappedRenames() {
        final Filter filter = new MatchFilter(MatchOptions.REGULAR, "Y", "A");
        verifyFilterIsPrioritized(filter.withDeclaredBarriers(new Object()));
    }

    @Test
    public void testRespectsBarrierWrappedRenames() {
        final Object barrier = new Object();
        final WhereFilter filter = new MatchFilter(MatchOptions.REGULAR, "Y", "A");
        verifyFilterIsPrioritized(ConjunctiveFilter.of(
                ConditionFilter.createConditionFilter("true").withDeclaredBarriers(barrier),
                filter.withRespectedBarriers(barrier)));
    }

    @Test
    public void testConjunctiveFilterRename() {
        // get crafty nesting a conjunctive filter inside of a disjunctive filter
        final WhereFilter filter = new MatchFilter(MatchOptions.REGULAR, "Y", "A");
        verifyFilterIsPrioritized(DisjunctiveFilter.of(
                ConditionFilter.createConditionFilter("false"),
                ConjunctiveFilter.of(
                        ConditionFilter.createConditionFilter("true"),
                        filter.withDeclaredBarriers(new Object()))));
    }


    @Test
    public void testNestedConjunctiveFilterRename() {
        // get crafty with some nesting
        final WhereFilter filter = new MatchFilter(MatchOptions.REGULAR, "Y", "A");
        verifyFilterIsPrioritized(ConjunctiveFilter.of(
                ConditionFilter.createConditionFilter("true"),
                ConjunctiveFilter.of(
                        ConditionFilter.createConditionFilter("true"),
                        filter.withDeclaredBarriers(new Object()))));
    }


    @Test
    public void testNestedDisjunctiveMultiFilterRename() {
        // goal of this test is to rename multiple inner filters via a top-level filter
        final WhereFilter filter0 = new MatchFilter(MatchOptions.REGULAR, "Y", "A");
        final WhereFilter filter1 = new MatchFilter(MatchOptions.REGULAR, "Y2", "A");
        verifyFilterIsPrioritized(DisjunctiveFilter.of(filter0, filter1));
    }

    /**
     * Tests that a DVT without any deferred filters can copy() itself correctly. Also tosses in a selectDistinct(), to
     * cover the case where it returns null instead of having special logic.
     */
    @Test
    public void testNoDeferredFiltersCopy() {
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
                .withAttributes(Collections.singletonMap("Key", "Value"))
                .where(filter1)
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 25000);

        final Table selectDistinct = source.selectDistinct("X");
        Assert.eq(selectDistinct.size(), "selectDistinct.size()", values.length);
    }

    /**
     * Tests that a DVT without any deferred filters can copy() itself correctly. Also tosses in a selectDistinct(), to
     * cover the case where it returns null instead of having special logic.
     */
    @Test
    public void testSelectDistinct() {
        testSelectDistinct(false);
        testSelectDistinct(true);
    }

    private void testSelectDistinct(final boolean coalesceFirst) {
        final String[] values = new String[] {"A", "B", "C", "D"};
        ExecutionContext.getContext().getQueryScope().putParam("values", values);

        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofString("X"),
                ColumnDefinition.ofLong("I"));
        final Table sourceTable = TableTools.emptyTable(100_000)
                .update("X = values[i % 4]", "I = ii");

        final Table source = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

        final Table unusedCoalescedTable;
        if (coalesceFirst) {
            unusedCoalescedTable = source.coalesce();
        } else {
            unusedCoalescedTable = null;
        }

        final Table selectDistinct = source.selectDistinct("X");
        Assert.eq(selectDistinct.size(), "selectDistinct.size()", values.length);

        if (coalesceFirst) {
            Assertions.assertThat(unusedCoalescedTable).isInstanceOf(QueryTable.class);
        }
    }

    @Test
    public void testSelectDistinctWithFilter() {
        final String[] values = new String[] {"A", "B", "C", "D"};
        ExecutionContext.getContext().getQueryScope().putParam("values", values);

        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofString("X"),
                ColumnDefinition.ofLong("I"));
        final Table sourceTable = TableTools.emptyTable(100_000)
                .update("X = values[i % 4]", "I = ii");

        final Table source = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

        final Table selectDistinct = source.where("X != `B`").selectDistinct("X");
        Assert.eq(selectDistinct.size(), "selectDistinct.size()", values.length - 1);
    }

    @Test
    public void testSelectDistinctArray() {
        final String[] values = new String[] {"A", "B", "C", "D"};
        ExecutionContext.getContext().getQueryScope().putParam("values", values);

        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofString("X"),
                ColumnDefinition.ofLong("I"));
        final Table sourceTable = TableTools.emptyTable(100_000)
                .update("X = values[i % 4]", "I = ii");

        final Table source = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

        final Table selectDistinct = source.selectDistinct("J=I_[i - 1]");
        TstUtils.assertTableEquals(sourceTable.selectDistinct("J=I_[i - 1]"), selectDistinct);
    }

    @Test
    public void testViewSimpleRetain() {
        final String[] values = new String[] {"A", "B", "C", "D"};
        ExecutionContext.getContext().getQueryScope().putParam("values", values);

        final Table sourceTable = TableTools.emptyTable(100_000)
                .update("X = values[i % 4]", "I = ii");

        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofString("X"),
                ColumnDefinition.ofLong("I"));

        final Table source = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

        final Table view1 = source.view("I", "X");

        Assertions.assertThat(view1).isInstanceOf(DeferredViewTable.class);

        final Table coalesced = view1.coalesce();
        TstUtils.assertTableEquals(sourceTable.view("I", "X"), coalesced);

        final Table view2 = view1.view("X", "K=I * 2");
        final Table view3 = view2.where("K > 10000");

        final Table coalesced3 = view3.coalesce();
        TstUtils.assertTableEquals(sourceTable.view("X", "K=I*2").where("K > 10000"), coalesced3);

        TstUtils.assertTableEquals(sourceTable.view("X", "K=I*2"), view2.coalesce());

        final Table whereAlreadyCoalesced = view2.where("X == `A`");
        TstUtils.assertTableEquals(sourceTable.view("X", "K=I*2").where("X==`A`"), whereAlreadyCoalesced);

        final Table view4 = view1.where();
        TstUtils.assertTableEquals(view1, view4);
    }

    @Test
    public void testWhereNoFilters() {
        final String[] values = new String[] {"A", "B", "C", "D"};
        ExecutionContext.getContext().getQueryScope().putParam("values", values);

        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofString("X"),
                ColumnDefinition.ofLong("I"));
        final Table sourceTable = TableTools.emptyTable(100_000)
                .update("X = values[i % 4]", "I = ii");

        final Table source = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

        final Table view1 = source.view("I", "X");

        Assertions.assertThat(view1).isInstanceOf(DeferredViewTable.class);

        final Table view2 = view1.where();
        TstUtils.assertTableEquals(sourceTable.view("I", "X"), view2);
    }

    @Test
    public void testViewThenDrop() {
        final String[] values = new String[] {"A", "B", "C", "D"};
        ExecutionContext.getContext().getQueryScope().putParam("values", values);

        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofString("X"),
                ColumnDefinition.ofLong("I"));
        final Table sourceTable = TableTools.emptyTable(100_000)
                .update("X = values[i % 4]", "I = ii");

        final Table source = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);

        final Table view1 = source.view("I", "X", "Y=X + `_`");
        final Table view2 = view1.dropColumns("X");

        Assertions.assertThat(view2).isInstanceOf(DeferredViewTable.class);

        final Table view3 = view2.where("Y=`B_`");
        TstUtils.assertTableEquals(sourceTable.where("X=`B`").view("I", "Y=X + `_`"), view3);
    }

    @Test
    public void testDeferredFilter() {
        final String[] values = new String[] {"A", "B", "C", "D"};
        ExecutionContext.getContext().getQueryScope().putParam("values", values);

        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofString("X"),
                ColumnDefinition.ofLong("I"));
        final Table sourceTable = TableTools.emptyTable(100_000)
                .update("X = values[i % 4]", "I = ii");

        final Table source = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                new WhereFilter[] {WhereFilterFactory.getExpression("X=`B`")});

        final Table view2 = source.view("K=I * 2");
        Assertions.assertThat(view2).isInstanceOf(DeferredViewTable.class);

        final Table coalesce = source.coalesce();
        final Table coalesce2 = view2.coalesce();

        TstUtils.assertTableEquals(sourceTable.where("X=`B`"), coalesce);
        TstUtils.assertTableEquals(sourceTable.where("X=`B`").view("K= I * 2"), coalesce2);
    }

    @Test
    public void testBadConstructor() {
        final String[] values = new String[] {"A", "B", "C", "D"};
        ExecutionContext.getContext().getQueryScope().putParam("values", values);

        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofString("X"),
                ColumnDefinition.ofLong("I"));
        final Table sourceTable = TableTools.emptyTable(100_000)
                .update("X = values[i % 4]", "I = ii");

        final IllegalStateException ise1 = assertThrows(IllegalStateException.class, () -> new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                new SelectColumn[] {SelectColumnFactory.getExpression("Y=X")},
                new WhereFilter[] {WhereFilterFactory.getExpression("X=`B`")}));
        assertEquals("Why do we have a view and a filter all at the same time?", ise1.getMessage());

        final IllegalStateException ise2 = assertThrows(IllegalStateException.class, () -> new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                new String[] {"I"},
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                new WhereFilter[] {WhereFilterFactory.getExpression("X=`B`")}));
        assertEquals("Why do we have a drop and a filter all at the same time?", ise2.getMessage());
    }

    private void verifyFilterIsPrioritized(final Filter filterToTest) {
        verifyFilterIsPrioritized(filterToTest, true, DeferredViewTableTest::mixedUpdate);
    }

    private static Table simpleUpdate(Table table) {
        return table.updateView("Y = X", "Y2 = Y", "I = I + 0");
    }

    private static Table mixedUpdate(Table table) {
        return table.updateView(List.of(new SourceColumn("X", "Y"), SelectColumnFactory.getExpression("Y2 = Y"),
                SelectColumnFactory.getExpression("I = I + 0")));
    }

    private static Table doubleRenameUpdate(Table table) {
        return table.updateView(List.of(new SourceColumn("X", "X2"), new SourceColumn("X2", "Y"),
                SelectColumnFactory.getExpression("Y2 = Y"), SelectColumnFactory.getExpression("I = I + 0")));
    }

    private static Table postViewSourceUpdate(Table table) {
        return table.updateView("Y = X", "Y2 = Y", "J = I + 0", "I=J");
    }

    private static Table longRenameUpdate(Table table) {
        return table.updateView("J = I");
    }

    private void verifyFilterIsPrioritized(final Filter filterToTest, final boolean expectsJump,
            final Function<Table, Table> updateFunction) {
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
        final DeferredViewTable originalDeferred = new DeferredViewTable(
                resultDef,
                "test",
                new DeferredViewTable.TableReference(sourceTable),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);
        final Table withUpdate = updateFunction.apply(originalDeferred);
        final Table deferredTable = withUpdate
                .where(ConjunctiveFilter.of(
                        filter1,
                        WhereFilter.of(filterToTest)))
                .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 6250);
        if (expectsJump) {
            final Table expected = withUpdate.where(filterToTest);
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
