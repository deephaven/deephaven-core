/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.engine.testutil.QueryTableTestBase;

import java.util.*;

import static io.deephaven.engine.util.TableTools.emptyTable;

public class SnapshotStateTest extends QueryTableTestBase {

    public void testTreeTableNotImplemented() {
        // TODO (https://github.com/deephaven/deephaven-core/issues/64): Delete this, uncomment and fix the rest
        try {
            emptyTable(10).tree("ABC", "DEF");
            fail("Expected exception");
        } catch (UnsupportedOperationException expected) {
        }
    }

    public void testRollupNotImplemented() {
        // TODO (https://github.com/deephaven/deephaven-core/issues/65): Delete this, uncomment and fix the rest
        try {
            emptyTable(10).rollup(List.of(), "ABC", "DEF");
            fail("Expected exception");
        } catch (UnsupportedOperationException expected) {
        }
    }

    // private static Table getRawNyMunis() throws CsvReaderException {
    // QueryLibrary.importStatic(TreeSnapshotQueryTest.StaticHolder.class);
    //
    // final BaseTable base =
    // (BaseTable) CsvTools.readCsv(TreeSnapshotQueryTest.class.getResourceAsStream("nymunis.csv"));
    // base.setRefreshing(true);
    // return base.update("Path=(List<String>)removeEmpty(County_Name, City_Name, Town_Name, Village_Name)")
    // .update("Direct = Path.size() == 1 ? null : new ArrayList(Path.subList(0, Path.size() - 1))")
    // .lastBy("Path");
    // }
    //
    // private static Table makeNyMunisTreeTableFrom(Table t) {
    // return t.tree("Path", "Direct");
    // }
    //
    // private static Table makeNyMunisTreeTable() throws CsvReaderException {
    // return makeNyMunisTreeTableFrom(getRawNyMunis());
    // }
    //
    // @Test
    // public void testBounds() throws CsvReaderException {
    // final HierarchicalTable tree = (HierarchicalTable) makeNyMunisTreeTable();
    // final Map<Object, TableDetails> details = new HashMap<>();
    //
    // final int columnCount = tree.getColumnSourceMap().size();
    //
    // // build a structure so we've got more to test than the trivial case
    // addTable(details, ROOT_TABLE_KEY, null, 7);
    // addTable(details, "parent", ROOT_TABLE_KEY, 5);
    // addTable(details, "child", "parent", 3);
    // addTable(details, "uncle", ROOT_TABLE_KEY, 2);
    //
    // // beginning of the tree, within bounds
    // SnapshotState state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 0, 4);
    // assertEquals(5, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(5, state.tableKeyColumn.length);
    //
    // // from middle of the tree to the very end
    // state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 12, 16);
    // assertEquals(5, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(5, state.tableKeyColumn.length);
    //
    // // full range of the tree
    // state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 0, 16);
    // assertEquals(17, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(17, state.tableKeyColumn.length);
    //
    // // range in the middle of the tree
    // state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 2, 8);
    // assertEquals(7, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(7, state.tableKeyColumn.length);
    //
    // // validation already exists in TSQ to prevent negative starts, so we skip that case
    //
    // // start midway, extend past the end
    // state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 8, 20);
    // assertEquals(9, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(9, state.tableKeyColumn.length);
    //
    // // start at the end, extend past the end
    // state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 16, 20);
    // assertEquals(1, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(1, state.tableKeyColumn.length);
    //
    // // start after the end, extend past the end
    // state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 20, 30);
    // assertEquals(0, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(0, state.tableKeyColumn.length);
    //
    // // start midway, extend way 2x past the end
    // state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 8, 40);
    // assertEquals(9, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(9, state.tableKeyColumn.length);
    //
    // // start at the end, extend 2x past the end
    // state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 16, 40);
    // assertEquals(1, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(1, state.tableKeyColumn.length);
    //
    // // start past the end, extend 2x past the end
    // state = new SnapshotState(tree, "Path");
    // state.beginSnapshot(details, new BitSet(0), 20, 40);
    // assertEquals(0, state.actualViewportSize);
    // assertEquals(17, state.totalRowCount);
    // assertEquals(columnCount, state.getDataMatrix().length);
    // assertEquals(0, state.tableKeyColumn.length);
    // }
    //
    // private void addTable(Map<Object, TableDetails> details, String key, String parentKey, int size) {
    // final TableDetails d = new TableDetails(key, new HashSet<>());
    // d.setTable(TableTools.emptyTable(size));
    // details.put(key, d);
    //
    // if (parentKey != null) {
    // final TableDetails parentDetails = details.get(parentKey);
    // assertNotNull("can't add a child before its parent", parentDetails);
    // parentDetails.getChildren().add(key);
    // }
    // }
}
