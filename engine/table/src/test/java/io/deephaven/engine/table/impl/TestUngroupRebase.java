/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;

import java.io.IOException;
import java.util.Random;

import static io.deephaven.engine.util.TableTools.intCol;

public class TestUngroupRebase extends RefreshingTableTestCase {
    public void testUngroupAgnosticRebase() throws IOException {
        int oldMinimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try {
            int size = 9;
            Random random = new Random(0);

            ColumnHolder arrayColumnHolder = TstUtils.c("Y", new int[] {10, 20}, new int[] {110, 120, 130});
            final QueryTable table = TstUtils.testRefreshingTable(TstUtils.c("X", 1, 3), arrayColumnHolder);

            EvalNugget en[] = new EvalNugget[] {
                    new EvalNugget() {
                        public Table e() {
                            return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(table::ungroup);
                        }
                    },
            };

            // don't remove or add anything, let's just do one step
            UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
            RowSet keysToAdd = RowSetFactory.empty();
            RowSet keysToRemove = RowSetFactory.empty();
            RowSet keysToModify = RowSetFactory.empty();
            table.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table);
            TstUtils.validate("ungroupRebase base", en);

            // Now let's modify the first row, but not cause a rebase
            UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
            keysToAdd = RowSetFactory.empty();
            keysToRemove = RowSetFactory.empty();
            keysToModify = RowSetFactory.fromKeys(0);
            ColumnHolder keyModifications = TstUtils.c("X", 1);
            ColumnHolder valueModifications = TstUtils.c("Y", new int[] {10, 20, 30});
            TstUtils.addToTable(table, keysToModify, keyModifications, valueModifications);
            table.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table);
            TstUtils.validate("ungroupRebase add no rebase", en);


            // Now let's modify the first row such that we will cause a rebasing operation
            UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
            keysToAdd = RowSetFactory.empty();
            keysToRemove = RowSetFactory.empty();
            keysToModify = RowSetFactory.fromKeys(0);
            valueModifications = TstUtils.c("Y", new int[] {10, 20, 30, 40, 50, 60});
            TstUtils.addToTable(table, keysToModify, keyModifications, valueModifications);
            table.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table);
            TstUtils.validate("ungroupRebase rebase", en);

            // Time to start fresh again, so we can do an addition operation,
            // without having such a high base for the table.
            arrayColumnHolder =
                    TstUtils.c("Y", new int[] {10, 20}, new int[] {200}, new int[] {110, 120, 130}, new int[] {310});
            final QueryTable table2 = TstUtils.testRefreshingTable(TstUtils.c("X", 1, 2, 3, 4), arrayColumnHolder);

            en = new EvalNugget[] {
                    new EvalNugget() {
                        public Table e() {
                            return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(table2::ungroup);
                        }
                    },
            };

            // let's remove the second row, so that we can add something to it on the next step
            UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
            keysToAdd = RowSetFactory.fromKeys();
            keysToRemove = RowSetFactory.fromKeys(1);
            keysToModify = RowSetFactory.fromKeys();
            TstUtils.removeRows(table2, keysToRemove);
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase remove", en);

            // now we want to add it back, causing a rebase, and modify another
            UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
            keysToAdd = RowSetFactory.fromKeys(1);
            keysToRemove = RowSetFactory.fromKeys();
            keysToModify = RowSetFactory.fromKeys(2, 3);

            ColumnHolder keyAdditions = TstUtils.c("X", 2);
            ColumnHolder valueAdditions = TstUtils.c("Y", new int[] {210, 220, 230, 240, 250, 260});
            valueModifications = TstUtils.c("Y", new int[] {110, 120, 140}, new int[] {320});
            keyModifications = TstUtils.c("X", 3, 4);

            TstUtils.addToTable(table2, keysToAdd, keyAdditions, valueAdditions);
            TstUtils.addToTable(table2, keysToModify, keyModifications, valueModifications);
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase add rebase", en);

            // an empty step
            UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
            keysToAdd = RowSetFactory.fromKeys();
            keysToRemove = RowSetFactory.fromKeys();
            keysToModify = RowSetFactory.fromKeys();
            TstUtils.addToTable(table2, keysToModify, intCol("X"), TstUtils.c("Y"));
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase add post rebase", en);

            // and another step, to make sure everything is fine post rebase
            UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
            keysToAdd = RowSetFactory.fromKeys();
            keysToRemove = RowSetFactory.fromKeys();
            keysToModify = RowSetFactory.fromKeys(2, 3);
            TstUtils.addToTable(table2, keysToModify, keyModifications, valueModifications);
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase add post rebase 2", en);

        } finally {
            QueryTable.setMinimumUngroupBase(oldMinimumUngroupBase);
        }
    }
}
