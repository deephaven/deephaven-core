package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableMap;
import io.deephaven.engine.table.TransformableTableMap;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;

import static io.deephaven.engine.table.impl.TstUtils.i;

public class TestEvenlyDividedTableMap extends RefreshingTableTestCase {
    public void testStatic() {
        final Table t = TableTools.emptyTable(1000000).update("K=k");
        final TableMap tm = EvenlyDividedTableMap.makeEvenlyDividedTableMap(t, 16, 100000);
        assertEquals(10, tm.size());
        final Table t2 = ((TransformableTableMap) tm.asTable().update("K2=K*2")).merge();
        TstUtils.assertTableEquals(t.update("K2=K*2"), t2);
    }

    public void testIncremental() {
        final QueryTable t = TstUtils.testRefreshingTable(RowSetFactory.flat(1000000).toTracking());
        final Table tu = UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> t.update("K=k*2"));
        final Table tk2 = UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> tu.update("K2=K*2"));
        final TableMap tm = EvenlyDividedTableMap.makeEvenlyDividedTableMap(tu, 16, 100000);
        assertEquals(10, tm.size());
        final Table t2 = UpdateGraphProcessor.DEFAULT.sharedLock()
                .computeLocked(() -> ((TransformableTableMap) tm.asTable().update("K2=K*2")).merge());
        TstUtils.assertTableEquals(tk2, t2);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final RowSet addedRows = RowSetFactory.fromRange(1000000, 1250000);
            TstUtils.addToTable(t, addedRows);
            t.notifyListeners(addedRows, i(), i());
        });

        TstUtils.assertTableEquals(tk2, t2);
        assertEquals(13, tm.size());
    }
}
