//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.sources.RowKeyColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(OutOfBandTest.class)
public class QueryTableWhereParallelTest extends QueryTableWhereTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // these are reset in parent class
        QueryTable.FORCE_PARALLEL_WHERE = true;
        QueryTable.DISABLE_PARALLEL_WHERE = false;
    }

    @Test
    public void testSplits() {
        final RowSetCapturingFilter recordingFilter = new RowSetCapturingFilter();

        QueryTable.PARALLEL_WHERE_SEGMENTS = 2;
        final Table ft = TableTools.emptyTable(1_000_000).where(recordingFilter);
        assertEquals(1_000_000, ft.size());
        assertEquals(new TLongArrayList(new long[] {500_000, 500_000}), getAndSortSizes(recordingFilter));

        recordingFilter.reset();
        final Table ft2 = TableTools.emptyTable(50_000).where(recordingFilter);
        assertEquals(50_000, ft2.size());
        assertEquals(new TLongArrayList(new long[] {50_000}), getAndSortSizes(recordingFilter));

        recordingFilter.reset();
        QueryTable.PARALLEL_WHERE_SEGMENTS = 4;
        final Table ft3 = TableTools.emptyTable(70_001).where(recordingFilter);
        assertEquals(70_001, ft3.size());
        assertEquals(new TLongArrayList(new long[] {35_000, 35_001}), getAndSortSizes(recordingFilter));

        recordingFilter.reset();
        QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT = 10_000;
        final Table ft4 = TableTools.emptyTable(69_999).where(recordingFilter);
        assertEquals(69_999, ft4.size());
        assertEquals(new TLongArrayList(new long[] {17_499, 17_500, 17_500, 17_500}),
                getAndSortSizes(recordingFilter));

        recordingFilter.reset();
        final Table ft5 = TableTools.emptyTable(69_999).where(recordingFilter.withSerial());
        assertEquals(69_999, ft5.size());
        assertEquals(new TLongArrayList(new long[] {69_999}), getAndSortSizes(recordingFilter));
    }

    @Test
    public void testParallelExecutionViaTableUpdate() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT = 1_000;
        final QueryTable table = TstUtils.testRefreshingTable(RowSetFactory.flat(1500).toTracking())
                .withAdditionalColumns(Map.of("K", new RowKeyColumnSource()));
        table.setRefreshing(true);
        table.setAttribute(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, true);
        final Table source = table.updateView("J = ii % 2 == 0 ? K : 0");

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> source.where("K == J")),
        };

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(1500, 2999);
            final RowSet modified = RowSetFactory.fromRange(0, 1499);
            table.getRowSet().writableCast().insert(added);

            final TableUpdate upstream = new TableUpdateImpl(
                    added, RowSetFactory.empty(), modified, RowSetShiftData.EMPTY, ModifiedColumnSet.ALL);

            table.notifyListeners(upstream);
        });

        // Ensure the table is as expected.
        TstUtils.validate(en);
    }
}
