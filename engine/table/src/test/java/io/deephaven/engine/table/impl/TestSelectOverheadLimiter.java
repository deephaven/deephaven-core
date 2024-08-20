//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;

import java.util.Random;

import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.TstUtils.*;

@Category(OutOfBandTest.class)
public class TestSelectOverheadLimiter extends RefreshingTableTestCase {
    public void testSelectOverheadLimiter() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(
                RowSetFactory.fromRange(0, 100).toTracking());
        final Table sentinelTable = queryTable.updateView("Sentinel=k");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Table densified = updateGraph.sharedLock().computeLocked(
                () -> SelectOverheadLimiter.clampSelectOverhead(sentinelTable, 3.0));
        assertEquals(densified.getRowSet(), sentinelTable.getRowSet());
        assertTableEquals(sentinelTable, densified);

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added3 = RowSetFactory.fromRange(10000, 11000);
            queryTable.getRowSet().writableCast().insert(added3);
            queryTable.notifyListeners(added3, i(), i());
        });

        assertEquals(sentinelTable.getRowSet(), densified.getRowSet());
        assertTableEquals(sentinelTable, densified);

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added2 = RowSetFactory.fromRange(11001, 11100);
            queryTable.getRowSet().writableCast().insert(added2);
            queryTable.notifyListeners(added2, i(), i());
        });

        assertEquals(sentinelTable.getRowSet(), densified.getRowSet());
        assertTableEquals(sentinelTable, densified);

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added1 = RowSetFactory.fromRange(20000, 20100);
            queryTable.getRowSet().writableCast().insert(added1);
            queryTable.notifyListeners(added1, i(), i());
        });

        assertEquals(sentinelTable.getRowSet(), densified.getRowSet());
        assertTableEquals(sentinelTable, densified);

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(30000, 30100);
            queryTable.getRowSet().writableCast().insert(added);
            queryTable.notifyListeners(added, i(), i());
        });

        assertEquals(sentinelTable.getRowSet(), densified.getRowSet());
        assertTableEquals(sentinelTable, densified);
    }

    public void testShift() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(
                RowSetFactory.fromRange(0, 100).toTracking());
        final Table sentinelTable = queryTable.updateView("Sentinel=ii");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Table densified = updateGraph.sharedLock().computeLocked(
                () -> SelectOverheadLimiter.clampSelectOverhead(sentinelTable, 3.0));
        assertEquals(densified.getRowSet(), sentinelTable.getRowSet());
        assertTableEquals(sentinelTable, densified);

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removed1 = RowSetFactory.fromRange(0, 100);
            final RowSet added = RowSetFactory.fromRange(10000, 10100);
            queryTable.getRowSet().writableCast().update(added, removed1);
            final TableUpdateImpl update = new TableUpdateImpl();
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(0, 1000, 10000);
            update.shifted = builder.build();
            update.added = i();
            update.removed = i();
            update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            queryTable.notifyListeners(update);
        });

        assertEquals(sentinelTable.getRowSet(), densified.getRowSet());
        assertTableEquals(sentinelTable, densified);

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removed = RowSetFactory.fromRange(10000, 10100);
            queryTable.getRowSet().writableCast().remove(removed);
            queryTable.notifyListeners(i(), removed, i());
        });
    }

    public void testPartitionBy() {
        SelectOverheadLimiter.conversions.set(0);
        int seed;
        for (seed = 0; seed < 50; ++seed) {
            System.out.println("Seed = " + seed);
            testPartitionBy(seed);
        }
        final int totalConversions = SelectOverheadLimiter.conversions.get();
        System.out.println("Total conversions: " + totalConversions);
        // we should have a good sampling of conversions; otherwise the test was not useful
        assertTrue(totalConversions > seed / 2);
        // but we know that we shouldn't have converted everything
        assertTrue(totalConversions < seed * 5);
    }

    private void testPartitionBy(int seed) {
        final Random random = new Random(seed);
        final int size = 10;

        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo[3];
        columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e"), "Sym",
                ColumnInfo.ColAttributes.Immutable);
        columnInfo[1] = new ColumnInfo<>(new IntGenerator(10, 20), "intCol",
                ColumnInfo.ColAttributes.Immutable);
        columnInfo[2] = new ColumnInfo<>(new SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable queryTable = getTable(size, random, columnInfo);
        final Table simpleTable = TableTools.newTable(TableTools.col("Sym", "a"), TableTools.intCol("intCol", 30),
                TableTools.doubleCol("doubleCol", 40.1)).updateView("K=-2L");
        final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();
        final Table source = updateGraph.sharedLock().computeLocked(
                () -> TableTools.merge(simpleTable, queryTable.updateView("K=k")).flatten());

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.Sorted.from(() -> updateGraph.sharedLock().computeLocked(
                        () -> SelectOverheadLimiter.clampSelectOverhead(source.partitionBy("Sym").merge(), 2.0)),
                        "Sym"),
                EvalNugget.Sorted.from(() -> updateGraph.sharedLock().computeLocked(
                        () -> SelectOverheadLimiter.clampSelectOverhead(source.partitionBy("Sym").merge(), 2.0)
                                .select()),
                        "Sym"),
                EvalNugget.Sorted.from(() -> updateGraph.sharedLock().computeLocked(
                        () -> SelectOverheadLimiter.clampSelectOverhead(source.partitionBy("Sym").merge(), 4.0)),
                        "Sym"),
                EvalNugget.Sorted.from(() -> updateGraph.sharedLock().computeLocked(
                        () -> SelectOverheadLimiter.clampSelectOverhead(source.partitionBy("Sym").merge(), 4.5)),
                        "Sym"),
                EvalNugget.Sorted.from(() -> updateGraph.sharedLock().computeLocked(
                        () -> SelectOverheadLimiter.clampSelectOverhead(source.partitionBy("Sym").merge(), 4.5)
                                .select()),
                        "Sym"),
                EvalNugget.Sorted.from(() -> updateGraph.sharedLock().computeLocked(
                        () -> SelectOverheadLimiter.clampSelectOverhead(source.partitionBy("Sym").merge(), 5.0)),
                        "Sym"),
                EvalNugget.Sorted.from(
                        () -> updateGraph.sharedLock()
                                .computeLocked(() -> SelectOverheadLimiter
                                        .clampSelectOverhead(source.partitionBy("Sym").merge(), 10.0).select()),
                        "Sym"),
                EvalNugget.Sorted.from(() -> updateGraph.sharedLock().computeLocked(
                        () -> SelectOverheadLimiter.clampSelectOverhead(source.partitionBy("Sym").merge(), 10.0)
                                .select()),
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
        final QueryTable queryTable = TstUtils.testRefreshingTable(
                RowSetFactory.fromRange(0, 100).toTracking());

        final SafeCloseable scopeCloseable = LivenessScopeStack.open();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Table sentinelTable = queryTable.updateView("Sentinel=k");
        final Table densified = updateGraph.sharedLock().computeLocked(
                () -> SelectOverheadLimiter.clampSelectOverhead(sentinelTable, 3.0));
        assertEquals(densified.getRowSet(), sentinelTable.getRowSet());
        assertTableEquals(sentinelTable, densified);

        final SingletonLivenessManager densifiedManager = new SingletonLivenessManager(densified);

        updateGraph.exclusiveLock().doLocked(scopeCloseable::close);

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added1 = RowSetFactory.fromRange(10000, 11000);
            queryTable.getRowSet().writableCast().insert(added1);
            queryTable.notifyListeners(added1, i(), i());
        });

        assertEquals(sentinelTable.getRowSet(), densified.getRowSet());
        assertTableEquals(sentinelTable, densified);

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(11001, 11100);
            queryTable.getRowSet().writableCast().insert(added);
            queryTable.notifyListeners(added, i(), i());
        });

        assertEquals(sentinelTable.getRowSet(), densified.getRowSet());
        assertTableEquals(sentinelTable, densified);

        org.junit.Assert.assertTrue(densified.tryRetainReference());
        org.junit.Assert.assertTrue(sentinelTable.tryRetainReference());

        densified.dropReference();
        sentinelTable.dropReference();

        updateGraph.exclusiveLock().doLocked(densifiedManager::release);

        org.junit.Assert.assertFalse(densified.tryRetainReference());
        org.junit.Assert.assertFalse(sentinelTable.tryRetainReference());
    }
}
