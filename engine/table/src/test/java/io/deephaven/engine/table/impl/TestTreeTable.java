//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper;
import io.deephaven.engine.table.impl.hierarchical.TreeTableFilter;
import io.deephaven.engine.table.impl.hierarchical.TreeTableImpl;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.sources.IntTestSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.deephaven.engine.testutil.HierarchicalTableTestTools.freeSnapshotTableChunks;
import static io.deephaven.engine.testutil.HierarchicalTableTestTools.snapshotToTable;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class TestTreeTable extends RefreshingTableTestCase {
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testRebase() {
        final Table source1 = TableTools.newTable(intCol("ID", 1, 2, 3, 4, 5),
                intCol("Parent", NULL_INT, 7, NULL_INT, 1, 1),
                intCol("Sentinel", 101, 102, 103, 104, 105));

        final TreeTable tree1a = source1.tree("ID", "Parent");
        final TreeTable.NodeOperationsRecorder recorder =
                tree1a.makeNodeOperationsRecorder().sortDescending("Sentinel");
        final TreeTable tree = tree1a.withNodeOperations(recorder);

        final Table keyTable = newTable(
                intCol(tree.getRowDepthColumn().name(), 0),
                intCol("ID", 1),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final HierarchicalTable.SnapshotState ss1 = tree.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(tree, ss1, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot);
        assertTableEquals(
                TableTools.newTable(intCol("ID", 3, 1, 5, 4), intCol("Parent", NULL_INT, NULL_INT, 1, 1),
                        intCol("Sentinel", 103, 101, 105, 104)),
                snapshot.view("ID", "Parent", "Sentinel"));
        freeSnapshotTableChunks(snapshot);

        final Table source2 = TreeTable.promoteOrphans(source1, "ID", "Parent");

        final TreeTable withAttributes = tree.withAttributes(Collections.singletonMap("Dog", "BestFriend"));

        final TreeTable rebased = withAttributes.rebase(source2);

        final HierarchicalTable.SnapshotState ss2 = rebased.makeSnapshotState();
        final Table snapshot2 =
                snapshotToTable(rebased, ss2, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot2);
        assertTableEquals(
                TableTools.newTable(intCol("ID", 3, 2, 1, 5, 4), intCol("Parent", NULL_INT, NULL_INT, NULL_INT, 1, 1),
                        intCol("Sentinel", 103, 102, 101, 105, 104)),
                snapshot2.view("ID", "Parent", "Sentinel"));
        freeSnapshotTableChunks(snapshot2);

        assertEquals(rebased.getAttribute("Dog"), "BestFriend");
    }

    @Test
    public void testFilterAfterSourceRowLookupStatesRemoved() {
        // The source row lookup is an aggregation by identifier. The filter reads a node's previous source row at the
        // row it looks up now, so the lookup's states must stay put even when aggregations move states.
        final double originalCollapse = ChunkedOperatorAggregationHelper.COLLAPSE_FREE_FRACTION;
        ChunkedOperatorAggregationHelper.COLLAPSE_FREE_FRACTION = 0.5;
        try {
            final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
            final int size = 2 * blockSize + 1;
            final long[] ids = new long[size];
            final long[] parents = new long[size];
            final int[] matches = new int[size];
            for (int ii = 0; ii < size; ++ii) {
                ids[ii] = ii;
                parents[ii] = ii == 2 * blockSize ? blockSize : NULL_LONG;
                matches[ii] = ii == 2 * blockSize ? 1 : 0;
            }
            final QueryTable source = testRefreshingTable(RowSetFactory.flat(size).toTracking(),
                    longCol("ID", ids), longCol("Parent", parents), intCol("M", matches));
            final TreeTable tree = source.tree("ID", "Parent");
            final Table filteredSource = tree.getSource().apply(new TreeTableFilter.Operator((TreeTableImpl) tree,
                    WhereFilterFactory.getExpressions("M == 1")));
            assertEquals(2, filteredSource.size());

            // a whole block of the lookup's states is removed, along with the matching node
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                final WritableRowSet removed = RowSetFactory.fromRange(0, blockSize - 1);
                removed.insert(2L * blockSize);
                removeRows(source, removed);
                source.notifyListeners(i(), removed, i());
            });
            assertEquals(0, filteredSource.size());
        } finally {
            ChunkedOperatorAggregationHelper.COLLAPSE_FREE_FRACTION = originalCollapse;
        }
    }

    @Test
    public void testFilterAfterParentAndChildRemoved() {
        // the filter reads the previous source row of a parent removed in the same cycle as its matching child
        final QueryTable source = testRefreshingTable(i(0, 1, 2).toTracking(),
                longCol("ID", 1, 2, 3), longCol("Parent", NULL_LONG, NULL_LONG, 2), intCol("M", 0, 0, 1));
        final TreeTable tree = source.tree("ID", "Parent");
        final Table filteredSource = tree.getSource().apply(new TreeTableFilter.Operator((TreeTableImpl) tree,
                WhereFilterFactory.getExpressions("M == 1")));
        assertEquals(2, filteredSource.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(source, i(1, 2));
            source.notifyListeners(i(), i(1, 2), i());
        });
        assertEquals(0, filteredSource.size());

        // the parent and child come back at new rows
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(source, i(5, 6), longCol("ID", 2, 3), longCol("Parent", NULL_LONG, 2), intCol("M", 0, 1));
            source.notifyListeners(i(5, 6), i(), i());
        });
        assertArrayEquals(new long[] {2, 3}, ColumnVectors.ofLong(filteredSource, "ID").toArray());
    }

    @Test
    public void testRebaseBadDef() {
        final Table source1 = TableTools.newTable(intCol("ID", 1, 2, 3, 4, 5),
                intCol("Parent", NULL_INT, 7, NULL_INT, 1, 1),
                intCol("Sentinel", 101, 102, 103, 104, 105));

        final TreeTable tree1a = source1.tree("ID", "Parent");
        final TreeTable.NodeOperationsRecorder recorder =
                tree1a.makeNodeOperationsRecorder().sortDescending("Sentinel");
        final TreeTable tree = tree1a.withNodeOperations(recorder);

        final Table source2 = source1.view("Parent", "ID", "Sentinel");
        final IllegalArgumentException iae =
                Assert.assertThrows(IllegalArgumentException.class, () -> tree.rebase(source2));
        assertEquals("Cannot rebase a TreeTable with a new source definition, column order is not identical",
                iae.getMessage());

        final Table source3 = source1.updateView("Extra=1");
        final IllegalArgumentException iae2 =
                Assert.assertThrows(IllegalArgumentException.class, () -> tree.rebase(source3));
        assertEquals(
                "Cannot rebase a TreeTable with a new source definition: new source column 'Extra' is missing in existing source",
                iae2.getMessage());
    }

    @Test
    public void testMismatchedParentAndId() {
        final Table source =
                emptyTable(10).update("ID=ii", "Parent=ii == 0 ? null : 63 - Long.numberOfLeadingZeros(ii)");

        final NoSuchColumnException missingParent =
                Assert.assertThrows(NoSuchColumnException.class, () -> source.tree("ID", "FooBar"));
        assertEquals("tree parent column: Unknown column names [FooBar], available column names are [ID, Parent]",
                missingParent.getMessage());
        final NoSuchColumnException missingId =
                Assert.assertThrows(NoSuchColumnException.class, () -> source.tree("FooBar", "Parent"));
        assertEquals("tree identifier column: Unknown column names [FooBar], available column names are [ID, Parent]",
                missingId.getMessage());

        final InvalidColumnException ice =
                Assert.assertThrows(InvalidColumnException.class, () -> source.tree("ID", "Parent"));
        assertEquals(
                "tree parent and identifier columns must have the same data type, but parent is [Parent, int] and identifier is [ID, long]",
                ice.getMessage());
    }

    @Test
    public void testDH21297() {
        final IntTestSource sentinel = new IntTestSource();
        final IntTestSource parent = new IntTestSource();

        final QueryTable source = new QueryTable(i().toTracking(),
                Map.of("Sentinel", sentinel, "Parent", parent));
        source.setRefreshing(true);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source,
                    i(0, 10, 11, 12),
                    col("Sentinel", 0, 1, 2, 3),
                    col("Parent", NULL_INT, 0, 0, 0));
            source.notifyListeners(i(0, 10, 11, 12), i(), i());
        });

        final TreeTable treed = source.tree("Sentinel", "Parent");

        final Table filteredSource = treed.getSource().apply(new TreeTableFilter.Operator((TreeTableImpl) treed,
                WhereFilterFactory.getExpressions("Sentinel in 1, 2, 3, 4, 5, 6")));

        showWithRowSet(source);
        showWithRowSet(filteredSource);

        assertArrayEquals(new int[] {0, 1, 2, 3}, ColumnVectors.ofInt(filteredSource, "Sentinel").toArray());

        System.out.println("\n\nAdding keys 4,5");

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source,
                    i(1, 21),
                    col("Sentinel", 4, 5),
                    col("Parent", NULL_INT, 4));
            source.notifyListeners(i(1, 21), i(), i());
        });

        showWithRowSet(source);
        showWithRowSet(filteredSource);

        assertArrayEquals(new int[] {0, 4, 1, 2, 3, 5}, ColumnVectors.ofInt(filteredSource, "Sentinel").toArray());

        System.out.println("\n\nShifting keys 0-1,+4");

        // Shift some parent rows
        updateGraph.runWithinUnitTestCycle(() -> {
            // shift the values
            parent.shift(0, 1, 4);
            sentinel.shift(0, 1, 4);

            // create the shift update
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(0, 1, 4);
            final RowSetShiftData shiftData = builder.build();

            shiftData.apply(source.getRowSet().writableCast());
            final TableUpdate update = new TableUpdateImpl(i(), i(), i(), shiftData, ModifiedColumnSet.EMPTY);
            source.notifyListeners(update);
        });

        showWithRowSet(source);
        showWithRowSet(filteredSource);

        assertArrayEquals(new int[] {0, 4, 1, 2, 3, 5}, ColumnVectors.ofInt(filteredSource, "Sentinel").toArray());

        System.out.println("\n\nShifting keys 4-5,-4 and 10-21,+1");

        // Shift some parent rows
        updateGraph.runWithinUnitTestCycle(() -> {
            // shift the values
            parent.shift(4, 5, -4);
            sentinel.shift(4, 5, -4);
            parent.shift(10, 21, 1);
            sentinel.shift(10, 21, 1);

            // create the shift update
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(4, 5, -4);
            builder.shiftRange(10, 21, 1);
            final RowSetShiftData shiftData = builder.build();

            shiftData.apply(source.getRowSet().writableCast());
            final TableUpdate update = new TableUpdateImpl(i(), i(), i(), shiftData, ModifiedColumnSet.EMPTY);
            source.notifyListeners(update);
        });

        showWithRowSet(source);
        showWithRowSet(filteredSource);

        assertArrayEquals(new int[] {0, 4, 1, 2, 3, 5}, ColumnVectors.ofInt(filteredSource, "Sentinel").toArray());
    }
}
