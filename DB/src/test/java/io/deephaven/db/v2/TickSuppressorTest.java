/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.TickSuppressor;
import io.deephaven.db.v2.utils.IndexShiftData;

import io.deephaven.test.types.OutOfBandTest;
import java.util.Random;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.intCol;
import static io.deephaven.db.v2.TstUtils.*;

@Category(OutOfBandTest.class)
public class TickSuppressorTest extends QueryTableTestBase {
    public void testModifyToAddRemoves() {
        final Random random = new Random(0);
        final ColumnInfo[] columnInfo;
        final int size = 50;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final Table sortedTable = queryTable.sort("intCol");

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> TickSuppressor.convertModificationsToAddsAndRemoves(queryTable)),
                EvalNugget.from(() -> TickSuppressor.convertModificationsToAddsAndRemoves(sortedTable)),
                EvalNugget.from(() -> TickSuppressor
                        .convertModificationsToAddsAndRemoves(TableTools.merge(queryTable, sortedTable))),
                EvalNugget.from(() -> TickSuppressor.convertModificationsToAddsAndRemoves(
                        queryTable.naturalJoin(queryTable.lastBy("Sym"), "Sym", "intCol2=intCol,doubleCol2=doubleCol")))
        };

        for (int i = 0; i < 50; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    public void testRemoveSpuriousModificationsIterative() {
        for (int seed = 0; seed < 1; ++seed) {
            testRemoveSpuriousModificationsIterative(seed, 100, 100);
        }
    }

    public void testRemoveSpuriousModificationsLargeIterative() {
        for (int seed = 0; seed < 1; ++seed) {
            testRemoveSpuriousModificationsIterative(seed, 32000, 5);
        }
    }

    private void testRemoveSpuriousModificationsIterative(int seed, int size, int maxSteps) {
        final Random random = new Random(seed);
        final ColumnInfo[] columnInfo;

        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol", "boolCol", "boolCol2"},
                        new SetGenerator<>("a", "b"),
                        new IntGenerator(0, 5),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new BooleanGenerator(0.5, 0.0),
                        new BooleanGenerator(0.95, 0.0)));

        final Table sortedTable = queryTable.sort("intCol");

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(queryTable)),
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(queryTable.view("boolCol"))),
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(queryTable.view("boolCol2"))),
                EvalNugget
                        .from(() -> TickSuppressor.removeSpuriousModifications(queryTable.view("boolCol", "boolCol2"))),
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(sortedTable)),
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(sortedTable.view("boolCol"))),
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(sortedTable.view("boolCol2"))),
                EvalNugget.from(
                        () -> TickSuppressor.removeSpuriousModifications(sortedTable.view("boolCol", "boolCol2"))),
                EvalNugget.from(
                        () -> TickSuppressor.removeSpuriousModifications(TableTools.merge(queryTable, sortedTable))),
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(queryTable
                        .naturalJoin(queryTable.lastBy("Sym"), "Sym", "intCol2=intCol,doubleCol2=doubleCol"))),
                EvalNugget.from(() -> queryTable.naturalJoin(
                        TickSuppressor.removeSpuriousModifications(queryTable.view("Sym", "boolCol").lastBy("Sym")),
                        "Sym", "jbc=boolCol")),
                EvalNugget.from(() -> queryTable.naturalJoin(
                        TickSuppressor.removeSpuriousModifications(queryTable.view("Sym", "boolCol2").lastBy("Sym")),
                        "Sym", "jbc2=boolCol2")),
                EvalNugget.from(() -> queryTable.naturalJoin(
                        TickSuppressor.removeSpuriousModifications(
                                queryTable.view("Sym", "boolCol", "boolCol2").lastBy("Sym")),
                        "Sym", "jbc=boolCol,jbc2=boolCol2")),
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(queryTable.naturalJoin(
                        TickSuppressor.removeSpuriousModifications(queryTable.view("Sym", "boolCol").lastBy("Sym")),
                        "Sym", "jbc=boolCol"))),
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(queryTable.naturalJoin(
                        TickSuppressor.removeSpuriousModifications(queryTable.view("Sym", "boolCol2").lastBy("Sym")),
                        "Sym", "jbc2=boolCol2"))),
                EvalNugget.from(() -> TickSuppressor.removeSpuriousModifications(queryTable.naturalJoin(
                        TickSuppressor.removeSpuriousModifications(
                                queryTable.view("Sym", "boolCol", "boolCol2").lastBy("Sym")),
                        "Sym", "jbc=boolCol,jbc2=boolCol2")))
        };

        for (int step = 0; step < maxSteps; step++) {
            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Seed = " + seed + ", Step = " + step);
            }
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    public void testRemoveSpuriousModifications() {
        final QueryTable input = TstUtils.testRefreshingTable(i(5, 10, 15),
                intCol("SentinelA", 5, 10, 15),
                intCol("SentinelB", 20, 30, 40));

        final DynamicTable suppressed = (DynamicTable) TickSuppressor.removeSpuriousModifications(input);

        final SimpleShiftAwareListener listener = new SimpleShiftAwareListener(suppressed);
        suppressed.listenForUpdates(listener);

        assertEquals(0, listener.getCount());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> input.notifyListeners(i(), i(), i(5)));

        assertEquals(0, listener.getCount());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(input, i(2, 5), intCol("SentinelA", 2, 5), intCol("SentinelB", 8, 11));
            input.notifyListeners(i(2), i(), i(5));
        });

        assertEquals(1, listener.getCount());
        assertEquals(i(2), listener.update.added);
        assertEquals(i(5), listener.update.modified);
        assertEquals(i(), listener.update.removed);
        assertEquals(IndexShiftData.EMPTY, listener.update.shifted);
        assertFalse(listener.update.modifiedColumnSet.containsAny(suppressed.newModifiedColumnSet("SentinelA")));
        assertTrue(listener.update.modifiedColumnSet.containsAny(suppressed.newModifiedColumnSet("SentinelB")));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(input, i(10, 15), intCol("SentinelA", 12, 15), intCol("SentinelB", 30, 40));
            removeRows(input, i(5));
            input.notifyListeners(i(), i(5), i(10, 15));
        });

        assertEquals(2, listener.getCount());
        assertEquals(i(), listener.update.added);
        assertEquals(i(10), listener.update.modified);
        assertEquals(i(5), listener.update.removed);
        assertEquals(IndexShiftData.EMPTY, listener.update.shifted);
        assertTrue(listener.update.modifiedColumnSet.containsAny(suppressed.newModifiedColumnSet("SentinelA")));
        assertFalse(listener.update.modifiedColumnSet.containsAny(suppressed.newModifiedColumnSet("SentinelB")));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(input, i(20), intCol("SentinelA", 20), intCol("SentinelB", 50));
            input.notifyListeners(i(20), i(), i());
        });

        assertEquals(3, listener.getCount());
        assertEquals(i(20), listener.update.added);
        assertEquals(i(), listener.update.modified);
        assertEquals(i(), listener.update.removed);
        assertEquals(IndexShiftData.EMPTY, listener.update.shifted);
        assertFalse(listener.update.modifiedColumnSet.containsAny(suppressed.newModifiedColumnSet("SentinelA")));
        assertFalse(listener.update.modifiedColumnSet.containsAny(suppressed.newModifiedColumnSet("SentinelB")));
    }
}
