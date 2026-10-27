//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
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
import io.deephaven.util.QueryConstants;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.ArrayList;
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
                ColumnDefinition.ofInt("X"),
                ColumnDefinition.ofInt("Y"));
        final Table sourceTable = TableTools.emptyTable(10);
        final SelectColumn[] viewColumns = SelectColumn.from(
                Selectable.parse("X = ii"),
                Selectable.parse("Y = 2 * X"));

        final DeferredViewTable deferredTable = DeferredViewTable.withView(resultDef, "test",
                new DeferredViewTable.TableReference(sourceTable), viewColumns);

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
                ColumnDefinition.ofInt("X"),
                ColumnDefinition.ofInt("Y"));
        final Table sourceTable = TableTools.emptyTable(10);
        if (sourceRefreshing) {
            sourceTable.setRefreshing(true);
        }

        final DeferredViewTable deferredTable =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null);

        Assert.eq(deferredTable.isRefreshing(), "deferredTable.isRefreshing()", sourceRefreshing, "sourceRefreshing");
    }

    @Test
    public void testIsRefreshingViaFilter() {
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofTime("Timestamp"));
        final Table sourceTable = TableTools.emptyTable(10).update("Timestamp = DateTimeUtils.now()");

        // We'll use a incremental release filter for convenience but any refreshing filter will do.
        final WhereFilter[] whereFilters = new WhereFilter[] {
                new IncrementalReleaseFilter(1, 1)
        };
        Assert.eqTrue(whereFilters[0].isRefreshing(), "whereFilters[0].isRefreshing()");

        final DeferredViewTable deferredTable = DeferredViewTable.withFilters(resultDef, "test",
                new DeferredViewTable.TableReference(sourceTable), whereFilters);

        Assert.eqTrue(deferredTable.isRefreshing(), "deferredTable.isRefreshing()");
    }

    @Test
    public void testUpdateColumnToDifferentType() {
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofLong("X"));
        final Table sourceTable = TableTools.emptyTable(10).update("X = ii");

        final SelectColumn[] viewColumns = SelectColumn.from(
                Selectable.parse("X = (int)X"));

        final DeferredViewTable deferredTable = DeferredViewTable.withView(resultDef, "test",
                new DeferredViewTable.TableReference(sourceTable), viewColumns);

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

        Table deferredTable =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null)
                        .updateView("Y = X")
                        .where(ConjunctiveFilter.of(serialFilter, freeFilter))
                        .coalesce();

        Assert.eq(deferredTable.size(), "deferredTable.size()", 25000);
        Assert.eq(numRowsFiltered(serialFilter), "numRowsFiltered(serialFilter)", 100_000);
        Assert.eq(numRowsFiltered(freeFilter), "numRowsFiltered(freeFilter)", 50_000);

        // ensure that it does jump over it if it is not serial
        final RowSetCapturingFilter notSerialFilter =
                new RowSetCapturingFilter(new RangeFilter("Y", Condition.LESS_THAN, "50000"));
        freeFilter.reset();

        deferredTable =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null)
                        .updateView("Y = X")
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
        final Table deferredTable =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null)
                        .updateView("Y = X")
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
        final Table deferredTable =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null)
                        .updateView("Y = X")
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
        Table deferredTable =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null)
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
    public void testFailedOverMatchFilterRename() {
        // a variable of the same name as the renamed column must not stand in for it pre-view
        ExecutionContext.getContext().getQueryScope().putParam("Rhs", 2);
        final TableDefinition resultDef = TableDefinition.of(
                ColumnDefinition.ofInt("Lhs"),
                ColumnDefinition.ofInt("Src"));
        final Table sourceTable = TableTools.newTable(
                TableTools.intCol("Lhs", 1, 2, 3),
                TableTools.intCol("Src", 1, 3, 3));
        final DeferredViewTable deferredTable =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null);

        // Rhs is a column, so Lhs == Rhs fails over to a ConditionFilter that uses Lhs and Rhs, and only Rhs is
        // renamed
        final Table result = deferredTable
                .view(List.of(new SourceColumn("Lhs"), new SourceColumn("Src", "Rhs")))
                .where("Lhs == Rhs")
                .coalesce();

        TstUtils.assertTableEquals(
                TableTools.newTable(TableTools.intCol("Lhs", 1, 3), TableTools.intCol("Rhs", 1, 3)),
                result);
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
        final Table source =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null)
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

        final Table source =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null);

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

        final Table source =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null);

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

        final Table source =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null);

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

        final Table source =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null);

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

        final Table source =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null);

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

        final Table source =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null);

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

        final Table source =
                DeferredViewTable.withFilters(resultDef, "test", new DeferredViewTable.TableReference(sourceTable),
                        new WhereFilter[] {WhereFilterFactory.getExpression("X=`B`")});

        final Table view2 = source.view("K=I * 2");
        Assertions.assertThat(view2).isInstanceOf(DeferredViewTable.class);

        final Table coalesce = source.coalesce();
        final Table coalesce2 = view2.coalesce();

        TstUtils.assertTableEquals(sourceTable.where("X=`B`"), coalesce);
        TstUtils.assertTableEquals(sourceTable.where("X=`B`").view("K= I * 2"), coalesce2);
    }

    @Test
    public void testRenameSwap() {
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("A = B", "B = A"));
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("C = A", "A = C"));
    }

    @Test
    public void testRenameSwapSameType() {
        final Table source = TableTools.newTable(
                TableTools.intCol("X", 1, 2, 3),
                TableTools.intCol("Y", 10, 20, 30));
        assertDeferredMatchesInMemory(source, t -> t.renameColumns("X = Y", "Y = X"));
    }

    @Test
    public void testRenameShift() {
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("B = A", "C = B"));
    }

    @Test
    public void testRenameRotation() {
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("A = B", "B = C", "C = A"));
    }

    @Test
    public void testRenameWithoutCollision() {
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("A_r = A", "B_r = B"));
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("B = A"));
    }

    @Test
    public void testRenameSwapTwice() {
        assertDeferredMatchesInMemory(renameSource(),
                t -> t.renameColumns("A = B", "B = A").renameColumns("A = B", "B = A"));
        assertDeferredMatchesInMemory(renameSource(),
                t -> t.renameColumns("A = B", "B = A").renameColumns("B = C", "C = B"));
    }

    @Test
    public void testRenameSwapThenMerge() {
        assertDeferredMatchesInMemory(renameSource(), t -> {
            final Table swapped = t.renameColumns("A = B", "B = A");
            return TableTools.merge(swapped, swapped);
        });
    }

    @Test
    public void testRenameSwapThenFilter() {
        for (final String filter : new String[] {"A == `y`", "B == 2", "A == `y` && B == 2", "A == `y` || C > 3"}) {
            assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("A = B", "B = A").where(filter));
        }
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("A = B", "B = A").where(Filter.or(
                FilterComparison.eq(ColumnName.of("A"), Literal.of("x")),
                FilterComparison.eq(ColumnName.of("B"), Literal.of(3)))));
    }

    @Test
    public void testRenameSwapFilterIsPushedDown() {
        final Table sourceTable = renameSource();
        final List<String> pushedColumns = new ArrayList<>();
        final DeferredViewTable.TableReference recordingReference = new DeferredViewTable.TableReference(sourceTable) {
            @Override
            protected TableAndRemainingFilters getWithWhere(final WhereFilter... whereFilters) {
                Arrays.stream(whereFilters).forEach(filter -> pushedColumns.addAll(filter.getColumns()));
                return super.getWithWhere(whereFilters);
            }
        };
        final Table swapped = DeferredViewTable.wrap(sourceTable.getDefinition(), "test", recordingReference, null)
                .renameColumns("A = B", "B = A");

        // a match filter and a formula filter, each on a swapped column, so each must reach the source table reading
        // the other column
        final Table result = swapped.where("A == `y`", "B + 0 == 2").coalesce();

        assertEquals(List.of("B", "A"), pushedColumns);
        TstUtils.assertTableEquals(
                sourceTable.renameColumns("A = B", "B = A").where("A == `y`", "B + 0 == 2"),
                result);
    }

    @Test
    public void testRenameSwapSelectDistinct() {
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("A = B", "B = A").selectDistinct("A"));
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("A = B", "B = A").selectDistinct("C"));
    }

    @Test
    public void testSequentialViewOverDeferredTable() {
        // unlike a rename, a view evaluates in order: a later column reads the result of an earlier column
        assertDeferredMatchesInMemory(renameSource(), t -> t.view("B = A", "A = B"));
        assertDeferredMatchesInMemory(renameSource(), t -> t.updateView("B = A", "A = B"));
        assertDeferredMatchesInMemory(renameSource(), t -> t.updateView("T = A", "A = B", "B = T"));
        assertDeferredMatchesInMemory(renameSource(), t -> t.view("B = A", "A = B").where("A == 2 && B == 2"));
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("D = C").view("B = A", "A = B"));
    }

    @Test
    public void testDisjunctionOfRenamedAndUnrenamedMatchFilters() {
        final Filter renamedNotNull = Filter.isNotNull(ColumnName.of("Renamed"));
        final Filter otherIsNull = Filter.isNull(ColumnName.of("Other"));
        final Filter[] filters = {
                Filter.or(renamedNotNull, otherIsNull),
                Filter.or(otherIsNull, renamedNotNull),
                Filter.or(FilterComparison.eq(ColumnName.of("Renamed"), Literal.of(1L)),
                        FilterComparison.eq(ColumnName.of("Other"), Literal.of("c"))),
                Filter.and(Filter.or(renamedNotNull, otherIsNull),
                        FilterComparison.neq(ColumnName.of("Renamed"), Literal.of(2L))),
        };
        for (final Function<Table, Table> rename : partialRenames()) {
            for (final Filter filter : filters) {
                assertDeferredMatchesInMemory(partialRenameSource(), rename.andThen(t -> t.where(filter)));
            }
        }
        // the value names a column, so the match filter fails over to a formula
        assertDeferredMatchesInMemory(partialRenameSource(), t -> t.renameColumns("Renamed = Value")
                .where(Filter.or(RawString.of("Renamed == Twin"), otherIsNull)));
    }

    @Test
    public void testFilterThroughNestedRenames() {
        final Function<Table, Table> renameThenDuplicate =
                t -> t.renameColumns("Renamed = Value").updateView("Dup = Renamed");
        for (final String filter : new String[] {
                "Renamed != null && Dup != null && Renamed == Dup",
                "Dup == 2",
                "Renamed == Dup && Other == `a`",
                "Renamed == null || Dup == 1"}) {
            assertDeferredMatchesInMemory(partialRenameSource(), renameThenDuplicate.andThen(t -> t.where(filter)));
        }
        assertDeferredMatchesInMemory(partialRenameSource(), renameThenDuplicate
                .andThen(t -> t.updateView("Dup2 = Dup").where("Renamed == Dup2 && Dup2 == 1")));
        // the value names a column, so the match filter fails over to a formula before the first rename
        assertDeferredMatchesInMemory(partialRenameSource(), t -> t.renameColumns("Renamed = Value", "Twin2 = Twin")
                .updateView("Dup = Renamed")
                .where("Dup == Twin2"));
    }

    @Test
    public void testFilterThroughSwapAfterRename() {
        // the swap maps P to Q and Q to P; the first rename maps Q to B and P to A, so P must end at B, not A
        assertDeferredMatchesInMemory(renameSource(), t -> t.renameColumns("P = A", "Q = B")
                .renameColumns("P = Q", "Q = P")
                .where("P.equals(`y`) && Q == 2"));
    }

    @Test
    public void testFilterThroughTwoSwaps() {
        // after both swaps, A reads the source's C, B reads the source's A, and C reads the source's B
        final Function<Table, Table> twoSwaps = t -> t.renameColumns("A = B", "B = A").renameColumns("A = C", "C = A");
        final Table source = TableTools.newTable(
                TableTools.intCol("A", 1, 4, 7),
                TableTools.intCol("B", 2, 5, 8),
                TableTools.intCol("C", 3, 6, 9));
        for (final String filter : new String[] {"B == 1 && A == 3", "A == 6", "C == 8", "A + C == 11"}) {
            assertDeferredMatchesInMemory(source, twoSwaps.andThen(t -> t.where(filter)));
        }
        // with three column types, a filter that reads the wrong column fails to compile or selects the wrong row
        assertDeferredMatchesInMemory(renameSource(),
                twoSwaps.andThen(t -> t.where("A > 2.0 && C.equals(`y`) && B == 2")));
    }

    @Test
    public void testFilterWithShiftedColumnsThroughSwap() {
        // a swap maps A to B and B to A, so both shifted columns must be renamed at once
        final Table source = TableTools.newTable(
                TableTools.intCol("A", 1, 2, 3, 4),
                TableTools.intCol("B", 10, 20, 30, 40));
        for (final String filter : new String[] {
                "A_[i - 1] != B_[i - 1]",
                "A_[i - 1] == 20",
                "B_[i + 1] == 3 || A_[i + 1] == 30"}) {
            assertDeferredMatchesInMemory(source, t -> t.renameColumns("A = B", "B = A").where(filter));
        }
    }

    @Test
    public void testFilterMixingShiftedAndPlainColumnsThroughRename() {
        final Table source = TableTools.newTable(
                TableTools.intCol("A", 0, 10, 20),
                TableTools.intCol("B", 10, 20, 30));
        assertDeferredMatchesInMemory(source, t -> t.renameColumns("A = B", "B = A").where("A_[i - 1] == B"));
        assertDeferredMatchesInMemory(source, t -> t.renameColumns("X = A").where("X_[i - 1] < X"));
    }

    @Test
    public void testShiftedFilterSeesTheRowsOfItsWhere() {
        // a filter with constant array access reads shifted columns built over the rows that the filter's where
        // receives, so no sibling filter of the same where may run first
        final Table source = TableTools.newTable(
                TableTools.intCol("A", 1, 2, 3),
                TableTools.intCol("C", 0, 3, 3));
        assertDeferredMatchesInMemory(source, t -> t.renameColumns("B = A").where("B_[i - 1] == 1", "C == 3"));
        assertDeferredMatchesInMemory(source, t -> t.renameColumns("B = A").where("C == 3").where("B_[i - 1] == 2"));
        // the shifted filter reads only a column that the rename passes through, and A names the query-scope variable
        ExecutionContext.getContext().getQueryScope().putParam("A", 0);
        assertDeferredMatchesInMemory(source, t -> t.renameColumns("B = A").where("C_[i - 1] == A"));
    }

    @Test
    public void testFilterOnColumnAndItsArrayThroughRename() {
        for (final Function<Table, Table> rename : partialRenames()) {
            for (final String filter : new String[] {
                    "Renamed == Renamed_[0]",
                    "Renamed_[0] == 1",
                    "Renamed == Renamed_[1]",
                    "Renamed == Renamed_[0] && Other == Other_[0]",
                    "Renamed == Renamed_[0] && Other == `a`"}) {
                assertDeferredMatchesInMemory(partialRenameSource(), rename.andThen(t -> t.where(filter)));
            }
        }
        assertDeferredMatchesInMemory(partialRenameSource(), t -> t.renameColumns("Renamed = Value", "Other2 = Other")
                .where("Renamed == Renamed_[0] && Other2 == Other2_[0]"));
    }

    @Test
    public void testFilterBeforeViewKeepsQueryScopeVariable() {
        // after each view, A names the query-scope variable; before the view, A names the source column that the view
        // hides
        ExecutionContext.getContext().getQueryScope().putParam("A", 5);
        final Table source = TableTools.newTable(
                TableTools.intCol("A", 1, 5, 7),
                TableTools.intCol("C", 5, 7, 5));
        final List<Function<Table, Table>> hidingA = List.of(
                t -> t.renameColumns("B = A"),
                t -> t.view("B = A", "C"));
        for (final Function<Table, Table> hide : hidingA) {
            for (final String filter : new String[] {
                    "B == A",
                    "B + 0 == A",
                    "B in A, 7",
                    "B_[1] == A",
                    "C == A",
                    "C + 0 == A",
                    "B == A || C == A",
                    "B == C"}) {
                assertDeferredMatchesInMemory(source, hide.andThen(t -> t.where(filter)));
            }
            assertDeferredMatchesInMemory(source, hide.andThen(t -> t.where(Filter.or(
                    FilterComparison.eq(ColumnName.of("B"), Literal.of(7)), RawString.of("C == A")))));
        }
        // two levels of renames, so the filter is renamed twice
        for (final String filter : new String[] {"D == A", "D + 0 == A", "D == A && C == A"}) {
            assertDeferredMatchesInMemory(source, t -> t.renameColumns("B = A").renameColumns("D = B").where(filter));
        }
    }

    private static Table renameSource() {
        return TableTools.newTable(
                TableTools.intCol("A", 1, 2, 3),
                TableTools.stringCol("B", "x", "y", "z"),
                TableTools.doubleCol("C", 1.5, 2.5, 3.5));
    }

    private static Table partialRenameSource() {
        return TableTools.newTable(
                TableTools.longCol("Value", 1L, 2L, QueryConstants.NULL_LONG),
                TableTools.stringCol("Other", "a", null, "c"),
                TableTools.longCol("Twin", 1L, 3L, QueryConstants.NULL_LONG));
    }

    private static List<Function<Table, Table>> partialRenames() {
        return List.of(
                t -> t.renameColumns("Renamed = Value"),
                t -> t.view("Renamed = Value", "Other"));
    }

    private static Table deferred(final Table source) {
        return DeferredViewTable.wrap(source.getDefinition(), "test", new DeferredViewTable.TableReference(source),
                null);
    }

    /**
     * Apply {@code operation} to {@code source} and to a {@link DeferredViewTable} over {@code source}, and verify that
     * the deferred result has the same definition before and after coalescing, and the same data.
     */
    private static void assertDeferredMatchesInMemory(final Table source, final Function<Table, Table> operation) {
        final Table expected = operation.apply(source);
        final Table actual = operation.apply(deferred(source));
        assertEquals(expected.getDefinition(), actual.getDefinition());
        final Table coalesced = actual.coalesce();
        assertEquals(expected.getDefinition(), coalesced.getDefinition());
        TstUtils.assertTableEquals(expected, coalesced);
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
        final DeferredViewTable originalDeferred =
                DeferredViewTable.wrap(resultDef, "test", new DeferredViewTable.TableReference(sourceTable), null);
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
