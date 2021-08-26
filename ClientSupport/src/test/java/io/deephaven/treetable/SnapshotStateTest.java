package io.deephaven.treetable;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.HierarchicalTable;
import io.deephaven.db.v2.QueryTableTestBase;
import org.junit.Test;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static io.deephaven.treetable.TreeTableConstants.ROOT_TABLE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SnapshotStateTest extends QueryTableTestBase {
    private static Table getRawNyMunis() throws IOException {
        QueryLibrary.importStatic(TreeSnapshotQueryTest.StaticHolder.class);

        final BaseTable base =
                (BaseTable) TableTools.readCsv(TreeSnapshotQueryTest.class.getResourceAsStream("nymunis.csv"));
        base.setRefreshing(true);
        return base.update("Path=(List<String>)removeEmpty(County_Name, City_Name, Town_Name, Village_Name)")
                .update("Direct = Path.size() == 1 ? null : new ArrayList(Path.subList(0, Path.size() - 1))")
                .lastBy("Path");
    }

    private static Table makeNyMunisTreeTableFrom(Table t) {
        return t.treeTable("Path", "Direct");
    }

    private static Table makeNyMunisTreeTable() throws IOException {
        return makeNyMunisTreeTableFrom(getRawNyMunis());
    }

    @Test
    public void testBounds() throws IOException {
        final HierarchicalTable treeTable = (HierarchicalTable) makeNyMunisTreeTable();
        final Map<Object, TableDetails> details = new HashMap<>();

        final int columnCount = treeTable.getColumnSourceMap().size();

        // build a structure so we've got more to test than the trivial case
        addTable(details, ROOT_TABLE_KEY, null, 7);
        addTable(details, "parent", ROOT_TABLE_KEY, 5);
        addTable(details, "child", "parent", 3);
        addTable(details, "uncle", ROOT_TABLE_KEY, 2);

        // beginning of the tree, within bounds
        SnapshotState state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 0, 4);
        assertEquals(5, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(5, state.tableKeyColumn.length);

        // from middle of the tree to the very end
        state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 12, 16);
        assertEquals(5, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(5, state.tableKeyColumn.length);

        // full range of the tree
        state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 0, 16);
        assertEquals(17, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(17, state.tableKeyColumn.length);

        // range in the middle of the tree
        state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 2, 8);
        assertEquals(7, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(7, state.tableKeyColumn.length);

        // validation already exists in TSQ to prevent negative starts, so we skip that case

        // start midway, extend past the end
        state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 8, 20);
        assertEquals(9, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(9, state.tableKeyColumn.length);

        // start at the end, extend past the end
        state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 16, 20);
        assertEquals(1, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(1, state.tableKeyColumn.length);

        // start after the end, extend past the end
        state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 20, 30);
        assertEquals(0, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(0, state.tableKeyColumn.length);

        // start midway, extend way 2x past the end
        state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 8, 40);
        assertEquals(9, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(9, state.tableKeyColumn.length);

        // start at the end, extend 2x past the end
        state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 16, 40);
        assertEquals(1, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(1, state.tableKeyColumn.length);

        // start past the end, extend 2x past the end
        state = new SnapshotState(treeTable, "Path");
        state.beginSnapshot(details, new BitSet(0), 20, 40);
        assertEquals(0, state.actualViewportSize);
        assertEquals(17, state.totalRowCount);
        assertEquals(columnCount, state.getDataMatrix().length);
        assertEquals(0, state.tableKeyColumn.length);
    }

    private void addTable(Map<Object, TableDetails> details, String key, String parentKey, int size) {
        final TableDetails d = new TableDetails(key, new HashSet<>());
        d.setTable(TableTools.emptyTable(size));
        details.put(key, d);

        if (parentKey != null) {
            final TableDetails parentDetails = details.get(parentKey);
            assertNotNull("can't add a child before its parent", parentDetails);
            parentDetails.getChildren().add(key);
        }
    }
}
