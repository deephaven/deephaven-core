/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;

public class TestBlinkTableTools {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testBlinkToAppendOnlyTable() {
        final Instant dt1 = DateTimeUtils.parseInstant("2021-08-11T8:20:00 NY");
        final Instant dt2 = DateTimeUtils.parseInstant("2021-08-11T8:21:00 NY");
        final Instant dt3 = DateTimeUtils.parseInstant("2021-08-11T11:22:00 NY");

        final QueryTable blinkTable = TstUtils.testRefreshingTable(i(1).toTracking(), intCol("I", 7),
                doubleCol("D", Double.NEGATIVE_INFINITY), instantCol("DT", dt1), col("B", Boolean.TRUE));
        blinkTable.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);

        final Table appendOnly = BlinkTableTools.blinkToAppendOnly(blinkTable);

        assertTableEquals(blinkTable, appendOnly);
        TestCase.assertEquals(true, appendOnly.getAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE));
        TestCase.assertTrue(appendOnly.isFlat());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            RowSet removed = blinkTable.getRowSet().copyPrev();
            ((WritableRowSet) blinkTable.getRowSet()).clear();
            TstUtils.addToTable(blinkTable, i(7), intCol("I", 1), doubleCol("D", Math.PI), instantCol("DT", dt2),
                    col("B", true));
            blinkTable.notifyListeners(i(7), removed, i());
        });

        assertTableEquals(TableTools.newTable(intCol("I", 7, 1), doubleCol("D", Double.NEGATIVE_INFINITY, Math.PI),
                instantCol("DT", dt1, dt2), col("B", true, true)), appendOnly);

        updateGraph.runWithinUnitTestCycle(() -> {
            RowSet removed = blinkTable.getRowSet().copyPrev();
            ((WritableRowSet) blinkTable.getRowSet()).clear();
            TstUtils.addToTable(blinkTable, i(7), intCol("I", 2), doubleCol("D", Math.E), instantCol("DT", dt3),
                    col("B", false));
            blinkTable.notifyListeners(i(7), removed, i());
        });
        assertTableEquals(
                TableTools.newTable(intCol("I", 7, 1, 2), doubleCol("D", Double.NEGATIVE_INFINITY, Math.PI, Math.E),
                        instantCol("DT", dt1, dt2, dt3), col("B", true, true, false)),
                appendOnly);
    }

    @Test
    public void testBlinkTableHasPrevData() throws InterruptedException {
        // we need to tell the UG that we intend to modify source tables mid-cycle
        final boolean sourceTablesAlreadySatisfied = false;

        final AtomicReference<QueryTable> result = new AtomicReference<>();
        final QueryTable blinkSource = createBlinkTableForConcurrentInstantiationTests();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(10, 19);
            final RowSet removed = blinkSource.getRowSet().copyPrev();
            ((WritableRowSet) blinkSource.getRowSet()).update(added, removed);

            createResultOffThread(blinkSource, result, Long.MAX_VALUE);

            TstUtils.assertTableEquals(TableTools.emptyTable(10).select("K = k"), result.get());
            blinkSource.notifyListeners(added, removed, i());
            updateGraph.markSourcesRefreshedForUnitTests();
        }, sourceTablesAlreadySatisfied);

        TstUtils.assertTableEquals(TableTools.emptyTable(20).select("K = k"), result.get());
    }

    @Test
    public void testBlinkTableHasCurrData() throws InterruptedException {
        // we need to tell the UG that we intend to modify source tables mid-cycle
        final boolean sourceTablesAlreadySatisfied = false;

        final AtomicReference<QueryTable> result = new AtomicReference<>();
        final QueryTable blinkSource = createBlinkTableForConcurrentInstantiationTests();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(10, 19);
            final RowSet removed = blinkSource.getRowSet().copyPrev();
            ((WritableRowSet) blinkSource.getRowSet()).update(added, removed);

            blinkSource.notifyListeners(added, removed, i());
            updateGraph.markSourcesRefreshedForUnitTests();

            createResultOffThread(blinkSource, result, Long.MAX_VALUE);
            TstUtils.assertTableEquals(TableTools.emptyTable(10).select("K = k + 10"), result.get());
        }, sourceTablesAlreadySatisfied);

        TstUtils.assertTableEquals(TableTools.emptyTable(10).select("K = k + 10"), result.get());
    }

    @Test
    public void testSizeLimitDuringInstantiation() throws InterruptedException {
        // we need to tell the UG that we intend to modify source tables mid-cycle
        final boolean sourceTablesAlreadySatisfied = false;

        final AtomicReference<QueryTable> result = new AtomicReference<>();
        final QueryTable blinkSource = createBlinkTableForConcurrentInstantiationTests();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(10, 19);
            final RowSet removed = blinkSource.getRowSet().copyPrev();
            ((WritableRowSet) blinkSource.getRowSet()).update(added, removed);

            createResultOffThread(blinkSource, result, 8);

            TstUtils.assertTableEquals(TableTools.emptyTable(8).select("K = k"), result.get());
            blinkSource.notifyListeners(added, removed, i());
            updateGraph.markSourcesRefreshedForUnitTests();
        }, sourceTablesAlreadySatisfied);

        // ensure that the append only table did not tick after instantiation
        Assert.assertTrue(result.get().getLastNotificationStep() < blinkSource.getLastNotificationStep());
        TstUtils.assertTableEquals(TableTools.emptyTable(8).select("K = k"), result.get());
    }

    @Test
    public void testSizeLimitDuringUpdate() throws InterruptedException {
        // we need to tell the UG that we intend to modify source tables mid-cycle
        final boolean sourceTablesAlreadySatisfied = false;

        final AtomicReference<QueryTable> result = new AtomicReference<>();
        final QueryTable blinkSource = createBlinkTableForConcurrentInstantiationTests();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(10, 14);
            final RowSet removed = blinkSource.getRowSet().copyPrev();
            ((WritableRowSet) blinkSource.getRowSet()).update(added, removed);

            createResultOffThread(blinkSource, result, 15);

            TstUtils.assertTableEquals(TableTools.emptyTable(10).select("K = k"), result.get());
            blinkSource.notifyListeners(added, removed, i());
            updateGraph.markSourcesRefreshedForUnitTests();
        }, sourceTablesAlreadySatisfied);

        final long lastNotificationStep = result.get().getLastNotificationStep();
        TstUtils.assertTableEquals(TableTools.emptyTable(15).select("K = k"), result.get());

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(15, 19);
            final RowSet removed = blinkSource.getRowSet().copyPrev();
            ((WritableRowSet) blinkSource.getRowSet()).update(added, removed);

            blinkSource.notifyListeners(added, removed, i());
            updateGraph.markSourcesRefreshedForUnitTests();
        }, sourceTablesAlreadySatisfied);

        // ensure that the append only table did not tick
        Assert.assertEquals(lastNotificationStep, result.get().getLastNotificationStep());
        TstUtils.assertTableEquals(TableTools.emptyTable(15).select("K = k"), result.get());
    }

    @Test
    public void testMemoKey() {
        // Note that EngineCleanup will clean this change during tearDown.
        QueryTable.setMemoizeResults(true);

        final QueryTable blinkSource = TstUtils.testRefreshingTable(RowSetFactory.empty().toTracking());
        blinkSource.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);

        final Object memoKey = new Object();
        final Object otherMemoKey = new Object();

        final Table r1;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            r1 = BlinkTableTools.blinkToAppendOnly(blinkSource);
            final Table r2 = BlinkTableTools.blinkToAppendOnly(blinkSource);
            Assert.assertSame(r1, r2);

            // test memo key
            final Table r_memo = BlinkTableTools.blinkToAppendOnly(blinkSource, memoKey);
            Assert.assertNotSame(r1, r_memo);

            // test another memo key
            final Table r_other_memo = BlinkTableTools.blinkToAppendOnly(blinkSource, otherMemoKey);
            Assert.assertNotSame(r1, r_other_memo);
            Assert.assertNotSame(r_memo, r_other_memo);

            // test different size limit
            final Table r_sz = BlinkTableTools.blinkToAppendOnly(blinkSource, 32);
            Assert.assertNotSame(r1, r_sz);

            // test reuse different size limit
            final Table r_sz_2 = BlinkTableTools.blinkToAppendOnly(blinkSource, 32);
            Assert.assertSame(r_sz, r_sz_2);

            // test memo key different size limit
            final Table r_sz_memo = BlinkTableTools.blinkToAppendOnly(blinkSource, 32, memoKey);
            Assert.assertNotSame(r_sz, r_sz_memo);

            // test another memo key
            final Table r_sz_other_memo = BlinkTableTools.blinkToAppendOnly(blinkSource, 32, otherMemoKey);
            Assert.assertNotSame(r_sz, r_sz_other_memo);
            Assert.assertNotSame(r_sz_memo, r_sz_other_memo);

            // test null memo key
            final Table r_null = BlinkTableTools.blinkToAppendOnly(blinkSource, null);
            Assert.assertNotSame(r1, r_null);

            // test that null memo key is not memoized
            final Table r_null_2 = BlinkTableTools.blinkToAppendOnly(blinkSource, null);
            Assert.assertNotSame(r1, r_null);
            Assert.assertNotSame(r_null, r_null_2);
        }

        // test that it was memoized only until the end of the liveness scope
        Assert.assertFalse(r1.tryRetainReference());
        final Table r2 = BlinkTableTools.blinkToAppendOnly(blinkSource);
        Assert.assertNotSame(r1, r2);
    }

    private static QueryTable createBlinkTableForConcurrentInstantiationTests() {
        final long[] colData = new long[20];
        for (int ii = 0; ii < colData.length; ++ii) {
            colData[ii] = ii;
        }
        final QueryTable blinkSource = TstUtils.testRefreshingTable(RowSetFactory.flat(colData.length).toTracking(),
                longCol("K", colData));
        blinkSource.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
        blinkSource.getRowSet().writableCast().clear();
        blinkSource.getRowSet().writableCast().insert(RowSetFactory.flat(10));
        blinkSource.getRowSet().writableCast().initializePreviousValue();
        return blinkSource;
    }

    private static void createResultOffThread(
            final QueryTable blinkSource,
            final AtomicReference<QueryTable> result,
            final long sizeLimit) throws InterruptedException {
        final ExecutionContext execContext = ExecutionContext.getContext();
        final Thread thread = new Thread(() -> {
            try (final SafeCloseable ignored = execContext.open()) {
                result.set((QueryTable) BlinkTableTools.blinkToAppendOnly(blinkSource, sizeLimit));
            }
        });
        thread.start();
        // don't forget to make this timeout generous when debugging
        thread.join(500);
    }
}
