package io.deephaven.db.v2;

import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.util.liveness.SingletonLivenessManager;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;

import java.util.Random;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.v2.TstUtils.*;

@Category(OutOfBandTest.class)
public class TestSelectOverheadLimiter extends LiveTableTestCase {
    public void testSelectOverheadLimiter() {
        final QueryTable queryTable =
            TstUtils.testRefreshingTable(Index.FACTORY.getIndexByRange(0, 100));
        final Table sentinelTable = queryTable.updateView("Sentinel=k");
        final Table densified = LiveTableMonitor.DEFAULT.sharedLock()
            .computeLocked(() -> SelectOverheadLimiter.clampSelectOverhead(sentinelTable, 3.0));
        assertEquals(densified.getIndex(), sentinelTable.getIndex());
        assertTableEquals(sentinelTable, densified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index added = Index.FACTORY.getIndexByRange(10000, 11000);
            queryTable.getIndex().insert(added);
            queryTable.notifyListeners(added, i(), i());
        });

        assertEquals(sentinelTable.getIndex(), densified.getIndex());
        assertTableEquals(sentinelTable, densified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index added = Index.FACTORY.getIndexByRange(11001, 11100);
            queryTable.getIndex().insert(added);
            queryTable.notifyListeners(added, i(), i());
        });

        assertEquals(sentinelTable.getIndex(), densified.getIndex());
        assertTableEquals(sentinelTable, densified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index added = Index.FACTORY.getIndexByRange(20000, 20100);
            queryTable.getIndex().insert(added);
            queryTable.notifyListeners(added, i(), i());
        });

        assertEquals(sentinelTable.getIndex(), densified.getIndex());
        assertTableEquals(sentinelTable, densified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index added = Index.FACTORY.getIndexByRange(30000, 30100);
            queryTable.getIndex().insert(added);
            queryTable.notifyListeners(added, i(), i());
        });

        assertEquals(sentinelTable.getIndex(), densified.getIndex());
        assertTableEquals(sentinelTable, densified);
    }

    public void testShift() {
        final QueryTable queryTable =
            TstUtils.testRefreshingTable(Index.FACTORY.getIndexByRange(0, 100));
        final Table sentinelTable = queryTable.updateView("Sentinel=ii");
        final Table densified = LiveTableMonitor.DEFAULT.sharedLock()
            .computeLocked(() -> SelectOverheadLimiter.clampSelectOverhead(sentinelTable, 3.0));
        assertEquals(densified.getIndex(), sentinelTable.getIndex());
        assertTableEquals(sentinelTable, densified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index removed = Index.FACTORY.getIndexByRange(0, 100);
            final Index added = Index.FACTORY.getIndexByRange(10000, 10100);
            queryTable.getIndex().update(added, removed);
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            final IndexShiftData.Builder builder = new IndexShiftData.Builder();
            builder.shiftRange(0, 1000, 10000);
            update.shifted = builder.build();
            update.added = update.removed = update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            queryTable.notifyListeners(update);
        });

        assertEquals(sentinelTable.getIndex(), densified.getIndex());
        assertTableEquals(sentinelTable, densified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index removed = Index.FACTORY.getIndexByRange(10000, 10100);
            queryTable.getIndex().remove(removed);
            queryTable.notifyListeners(i(), removed, i());
        });
    }

    public void testByExternal() {
        SelectOverheadLimiter.conversions.set(0);
        int seed;
        for (seed = 0; seed < 50; ++seed) {
            System.out.println("Seed = " + seed);
            testByExternal(seed);
        }
        final int totalConversions = SelectOverheadLimiter.conversions.get();
        System.out.println("Total conversions: " + totalConversions);
        // we should have a good sampling of conversions; otherwise the test was not useful
        assertTrue(totalConversions > seed / 2);
        // but we know that we shouldn't have converted everything
        assertTrue(totalConversions < seed * 5);
    }

    private void testByExternal(int seed) {
        final Random random = new Random(seed);
        final int size = 10;

        final TstUtils.ColumnInfo[] columnInfo = new TstUtils.ColumnInfo[3];
        columnInfo[0] =
            new TstUtils.ColumnInfo<>(new TstUtils.SetGenerator<>("a", "b", "c", "d", "e"), "Sym",
                TstUtils.ColumnInfo.ColAttributes.Immutable);
        columnInfo[1] = new TstUtils.ColumnInfo<>(new TstUtils.IntGenerator(10, 20), "intCol",
            TstUtils.ColumnInfo.ColAttributes.Immutable);
        columnInfo[2] =
            new TstUtils.ColumnInfo<>(new TstUtils.SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable queryTable = getTable(size, random, columnInfo);
        final Table simpleTable =
            TableTools.newTable(TableTools.col("Sym", "a"), TableTools.intCol("intCol", 30),
                TableTools.doubleCol("doubleCol", 40.1)).updateView("K=-2L");
        final Table source = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(
            () -> TableTools.merge(simpleTable, queryTable.updateView("K=k")).flatten());

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.Sorted
                    .from(
                        () -> LiveTableMonitor.DEFAULT.sharedLock()
                            .computeLocked(() -> SelectOverheadLimiter
                                .clampSelectOverhead(source.byExternal("Sym").merge(), 2.0)),
                        "Sym"),
                EvalNugget.Sorted.from(
                    () -> LiveTableMonitor.DEFAULT.sharedLock()
                        .computeLocked(() -> SelectOverheadLimiter
                            .clampSelectOverhead(source.byExternal("Sym").merge(), 2.0).select()),
                    "Sym"),
                EvalNugget.Sorted
                    .from(
                        () -> LiveTableMonitor.DEFAULT.sharedLock()
                            .computeLocked(() -> SelectOverheadLimiter
                                .clampSelectOverhead(source.byExternal("Sym").merge(), 4.0)),
                        "Sym"),
                EvalNugget.Sorted
                    .from(
                        () -> LiveTableMonitor.DEFAULT.sharedLock()
                            .computeLocked(() -> SelectOverheadLimiter
                                .clampSelectOverhead(source.byExternal("Sym").merge(), 4.5)),
                        "Sym"),
                EvalNugget.Sorted.from(
                    () -> LiveTableMonitor.DEFAULT.sharedLock()
                        .computeLocked(() -> SelectOverheadLimiter
                            .clampSelectOverhead(source.byExternal("Sym").merge(), 4.5).select()),
                    "Sym"),
                EvalNugget.Sorted
                    .from(
                        () -> LiveTableMonitor.DEFAULT.sharedLock()
                            .computeLocked(() -> SelectOverheadLimiter
                                .clampSelectOverhead(source.byExternal("Sym").merge(), 5.0)),
                        "Sym"),
                EvalNugget.Sorted.from(
                    () -> LiveTableMonitor.DEFAULT
                        .sharedLock()
                        .computeLocked(() -> SelectOverheadLimiter
                            .clampSelectOverhead(source.byExternal("Sym").merge(), 10.0).select()),
                    "Sym"),
                EvalNugget.Sorted.from(
                    () -> LiveTableMonitor.DEFAULT.sharedLock()
                        .computeLocked(() -> SelectOverheadLimiter
                            .clampSelectOverhead(source.byExternal("Sym").merge(), 10.0).select()),
                    "Sym"),
        };

        final int steps = 10;
        for (int step = 0; step < steps; step++) {
            if (printTableUpdates) {
                System.out.println("Step = " + step);
            }
            simulateShiftAwareStep("step == " + step, size, random, queryTable, columnInfo, en);
        }
    }

    public void testScope() {
        final QueryTable queryTable =
            TstUtils.testRefreshingTable(Index.FACTORY.getIndexByRange(0, 100));

        final SafeCloseable scopeCloseable = LivenessScopeStack.open();

        final Table sentinelTable = queryTable.updateView("Sentinel=k");
        final Table densified = LiveTableMonitor.DEFAULT.sharedLock()
            .computeLocked(() -> SelectOverheadLimiter.clampSelectOverhead(sentinelTable, 3.0));
        assertEquals(densified.getIndex(), sentinelTable.getIndex());
        assertTableEquals(sentinelTable, densified);

        final SingletonLivenessManager densifiedManager = new SingletonLivenessManager(densified);

        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(scopeCloseable::close);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index added = Index.FACTORY.getIndexByRange(10000, 11000);
            queryTable.getIndex().insert(added);
            queryTable.notifyListeners(added, i(), i());
        });

        assertEquals(sentinelTable.getIndex(), densified.getIndex());
        assertTableEquals(sentinelTable, densified);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index added = Index.FACTORY.getIndexByRange(11001, 11100);
            queryTable.getIndex().insert(added);
            queryTable.notifyListeners(added, i(), i());
        });

        assertEquals(sentinelTable.getIndex(), densified.getIndex());
        assertTableEquals(sentinelTable, densified);

        org.junit.Assert.assertTrue(densified.tryRetainReference());
        org.junit.Assert.assertTrue(sentinelTable.tryRetainReference());

        densified.dropReference();
        sentinelTable.dropReference();

        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(densifiedManager::release);

        org.junit.Assert.assertFalse(densified.tryRetainReference());
        org.junit.Assert.assertFalse(sentinelTable.tryRetainReference());
    }
}
