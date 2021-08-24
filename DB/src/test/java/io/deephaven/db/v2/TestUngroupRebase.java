/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.utils.ColumnHolder;
import io.deephaven.db.v2.utils.Index;

import java.io.IOException;
import java.util.Random;

import static io.deephaven.db.tables.utils.TableTools.intCol;

public class TestUngroupRebase extends LiveTableTestCase {
    public void testUngroupAgnosticRebase() throws IOException {
        int oldMinimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try {
            int size = 9;
            Random random = new Random(0);

            ColumnHolder arrayColumnHolder =
                TstUtils.c("Y", new int[] {10, 20}, new int[] {110, 120, 130});
            final QueryTable table =
                TstUtils.testRefreshingTable(TstUtils.c("X", 1, 3), arrayColumnHolder);

            EvalNugget en[] = new EvalNugget[] {
                    new EvalNugget() {
                        public Table e() {
                            return LiveTableMonitor.DEFAULT.exclusiveLock()
                                .computeLocked(table::ungroup);
                        }
                    },
            };

            // don't remove or add anything, let's just do one step
            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            Index keysToRemove = Index.FACTORY.getEmptyIndex();
            Index keysToAdd = Index.FACTORY.getEmptyIndex();
            Index keysToModify = Index.FACTORY.getEmptyIndex();
            table.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table);
            TstUtils.validate("ungroupRebase base", en);

            // Now let's modify the first row, but not cause a rebase
            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            keysToModify = Index.FACTORY.getIndexByValues(0);
            ColumnHolder keyModifications = TstUtils.c("X", 1);
            ColumnHolder valueModifications = TstUtils.c("Y", new int[] {10, 20, 30});
            TstUtils.addToTable(table, keysToModify, keyModifications, valueModifications);
            table.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table);
            TstUtils.validate("ungroupRebase add no rebase", en);


            // Now let's modify the first row such that we will cause a rebasing operation
            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            keysToModify = Index.FACTORY.getIndexByValues(0);
            valueModifications = TstUtils.c("Y", new int[] {10, 20, 30, 40, 50, 60});
            TstUtils.addToTable(table, keysToModify, keyModifications, valueModifications);
            table.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table);
            TstUtils.validate("ungroupRebase rebase", en);

            // Time to start fresh again, so we can do an addition operation,
            // without having such a high base for the table.
            arrayColumnHolder = TstUtils.c("Y", new int[] {10, 20}, new int[] {200},
                new int[] {110, 120, 130}, new int[] {310});
            final QueryTable table2 =
                TstUtils.testRefreshingTable(TstUtils.c("X", 1, 2, 3, 4), arrayColumnHolder);

            en = new EvalNugget[] {
                    new EvalNugget() {
                        public Table e() {
                            return LiveTableMonitor.DEFAULT.exclusiveLock()
                                .computeLocked(table2::ungroup);
                        }
                    },
            };

            // let's remove the second row, so that we can add something to it on the next step
            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            keysToRemove = Index.FACTORY.getIndexByValues(1);
            keysToAdd = Index.FACTORY.getIndexByValues();
            keysToModify = Index.FACTORY.getIndexByValues();
            TstUtils.removeRows(table2, keysToRemove);
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase remove", en);

            // now we want to add it back, causing a rebase, and modify another
            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            keysToRemove = Index.FACTORY.getIndexByValues();
            keysToAdd = Index.FACTORY.getIndexByValues(1);
            keysToModify = Index.FACTORY.getIndexByValues(2, 3);

            ColumnHolder keyAdditions = TstUtils.c("X", 2);
            ColumnHolder valueAdditions = TstUtils.c("Y", new int[] {210, 220, 230, 240, 250, 260});
            valueModifications = TstUtils.c("Y", new int[] {110, 120, 140}, new int[] {320});
            keyModifications = TstUtils.c("X", 3, 4);

            TstUtils.addToTable(table2, keysToAdd, keyAdditions, valueAdditions);
            TstUtils.addToTable(table2, keysToModify, keyModifications, valueModifications);
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase add rebase", en);

            // an empty step
            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            keysToRemove = Index.FACTORY.getIndexByValues();
            keysToAdd = Index.FACTORY.getIndexByValues();
            keysToModify = Index.FACTORY.getIndexByValues();
            TstUtils.addToTable(table2, keysToModify, intCol("X"), TstUtils.c("Y"));
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase add post rebase", en);

            // and another step, to make sure everything is fine post rebase
            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            keysToRemove = Index.FACTORY.getIndexByValues();
            keysToAdd = Index.FACTORY.getIndexByValues();
            keysToModify = Index.FACTORY.getIndexByValues(2, 3);
            TstUtils.addToTable(table2, keysToModify, keyModifications, valueModifications);
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase add post rebase 2", en);

        } finally {
            QueryTable.setMinimumUngroupBase(oldMinimumUngroupBase);
        }
    }
}
