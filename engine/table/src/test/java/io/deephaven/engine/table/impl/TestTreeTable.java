//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.HierarchicalTableTestTools.freeSnapshotTableChunks;
import static io.deephaven.engine.testutil.HierarchicalTableTestTools.snapshotToTable;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;

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
}
