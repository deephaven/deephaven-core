/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.table.impl.DataAccessHelpers;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.*;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;

import java.util.Map;
import java.util.Set;

import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.testutil.TstUtils.*;

@Category(OutOfBandTest.class)
public class TestSyncTableFilter extends RefreshingTableTestCase {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        setExpectError(false);
    }

    public void testSimple() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder("ID");
        builder.addTable("a", a);
        builder.addTable("b", b);
        final Map<String, Table> result = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(Set.of("a", "b"), result.keySet());

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        TableTools.show(fa);
        TableTools.show(fb);

        final Table ex1a = newTable(longCol("ID", 2, 2), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
        final Table ex1b = newTable(longCol("ID", 2, 2), intCol("Sentinel", 203, 204), col("Key", "a", "a"));

        assertTableEquals(fa, ex1a);
        assertTableEquals(fb, ex1b);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
            a.notifyListeners(i(10, 11), i(), i());
        });

        assertTableEquals(fa, ex1a);
        assertTableEquals(fb, ex1b);

        final Table ex2a = newTable(longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
        final Table ex2b = newTable(longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));
            b.notifyListeners(i(10, 11), i(), i());
        });

        showWithRowSet(fa);
        showWithRowSet(fb);

        assertTableEquals(fa, ex2a);
        assertTableEquals(fb, ex2b);
    }

    public void testSimpleAddAgain() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder("ID");
        builder.addTable("a", a);
        builder.addTable("b", b);
        final Map<String, Table> result =
                ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(builder::build);

        assertEquals(Set.of("a", "b"), result.keySet());

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        TableTools.show(fa);
        TableTools.show(fb);

        final Table ex1a = newTable(longCol("ID", 2, 2), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
        final Table ex1b = newTable(longCol("ID", 2, 2), intCol("Sentinel", 203, 204), col("Key", "a", "a"));

        assertTableEquals(fa, ex1a);
        assertTableEquals(fb, ex1b);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
            a.notifyListeners(i(10, 11), i(), i());
        });

        assertTableEquals(fa, ex1a);
        assertTableEquals(fb, ex1b);

        final Table ex2a = newTable(longCol("ID", 5, 5, 5, 5), intCol("Sentinel", 107, 108, 109, 110),
                col("Key", "b", "b", "c", "c"));
        final Table ex2b = newTable(longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));
            b.notifyListeners(i(10, 11), i(), i());
            TstUtils.addToTable(a, i(12, 13), longCol("ID", 5, 5), intCol("Sentinel", 109, 110), col("Key", "c", "c"));
            a.notifyListeners(i(12, 13), i(), i());
        });

        showWithRowSet(fa);
        showWithRowSet(fb);

        assertTableEquals(fa, ex2a);
        assertTableEquals(fb, ex2b);

        final Table ex3b =
                newTable(longCol("ID", 5, 5, 5), intCol("Sentinel", 207, 208, 209), col("Key", "a", "a", "a"));

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(12, 13), longCol("ID", 5, 6), intCol("Sentinel", 209, 210), col("Key", "a", "a"));
            b.notifyListeners(i(12, 13), i(), i());
        });

        showWithRowSet(fa);
        showWithRowSet(fb);

        assertTableEquals(fa, ex2a);
        assertTableEquals(fb, ex3b);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(14, 15), longCol("ID", 5, 6), intCol("Sentinel", 111, 112), col("Key", "a", "a"));
            a.notifyListeners(i(14, 15), i(), i());
        });

        System.out.println("A advanced to 6");
        showWithRowSet(fa);
        showWithRowSet(fb);

        final Table ex4a = newTable(longCol("ID", 6), intCol("Sentinel", 112), col("Key", "a"));
        final Table ex4b = newTable(longCol("ID", 6), intCol("Sentinel", 210), col("Key", "a"));

        assertTableEquals(fa, ex4a);
        assertTableEquals(fb, ex4b);
    }

    public void testNullAppearance() {
        final QueryTable a = TstUtils.testRefreshingTable(
                longCol("ID", 1, 1, QueryConstants.NULL_LONG, QueryConstants.NULL_LONG, 3, 3),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder().defaultId("ID").defaultKeys()
                .addTable("a", a)
                .addTable("b", b, "ID");
        final Map<String, Table> result =
                ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(builder::build);

        assertEquals(Set.of("a", "b"), result.keySet());

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        TableTools.show(fa);
        TableTools.show(fb);

        final Table empty = a.getSubTable(i().toTracking());

        assertTableEquals(fa, empty);
        assertTableEquals(fb, empty);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
            TstUtils.addToTable(a, i(2, 3), longCol("ID", 2, 2), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
            a.notifyListeners(i(10, 11), i(), i(2, 3));
        });

        final Table ex1a = newTable(longCol("ID", 2, 2), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
        final Table ex1b = newTable(longCol("ID", 2, 2), intCol("Sentinel", 203, 204), col("Key", "a", "a"));

        assertTableEquals(fa, ex1a);
        assertTableEquals(fb, ex1b);

        final Table ex2a = newTable(longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
        final Table ex2b = newTable(longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));
            b.notifyListeners(i(10, 11), i(), i());
        });

        showWithRowSet(fa);
        showWithRowSet(fb);

        assertTableEquals(fa, ex2a);
        assertTableEquals(fb, ex2b);
    }

    public void testSimpleKeyed() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3, 4, 4, 5, 5),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106, 107, 108, 109, 110),
                col("Key", "a", "a", "b", "b", "a", "a", "a", "a", "b", "b"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("Ego", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Klyuch", "a", "a", "b", "b", "a", "a"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder();
        builder.addTable("a", a, "ID", "Key");
        builder.addTable("b", b, "Ego", "Klyuch");
        final Map<String, Table> result =
                ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(builder::build);

        assertEquals(Set.of("a", "b"), result.keySet());

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        final TableUpdateValidator tuvfa = TableUpdateValidator.make("fa", (QueryTable) fa);
        final FailureListener fla = new FailureListener();
        tuvfa.getResultTable().addUpdateListener(fla);
        final TableUpdateValidator tuvfb = TableUpdateValidator.make("fa", (QueryTable) fb);
        final FailureListener flb = new FailureListener();
        tuvfb.getResultTable().addUpdateListener(flb);

        TableTools.show(fa);
        TableTools.show(fb);

        final Table ex1a = newTable(longCol("ID", 2, 2, 4, 4), intCol("Sentinel", 103, 104, 107, 108),
                col("Key", "b", "b", "a", "a"));
        final Table ex1b = newTable(longCol("Ego", 2, 2, 4, 4), intCol("Sentinel", 203, 204, 205, 206),
                col("Klyuch", "b", "b", "a", "a"));

        assertTableEquals(fa, ex1a);
        assertTableEquals(fb, ex1b);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10, 11), longCol("Ego", 5, 5), intCol("Sentinel", 207, 208),
                    col("Klyuch", "b", "c"));
            b.notifyListeners(i(10, 11), i(), i());
        });

        final Table ex2a = newTable(longCol("ID", 4, 4, 5, 5), intCol("Sentinel", 107, 108, 109, 110),
                col("Key", "a", "a", "b", "b"));
        final Table ex2b =
                newTable(longCol("Ego", 4, 4, 5), intCol("Sentinel", 205, 206, 207), col("Klyuch", "a", "a", "b"));

        assertTableEquals(fa, ex2a);
        assertTableEquals(fb, ex2b);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(20, 21), longCol("ID", 5, 5), intCol("Sentinel", 111, 112), col("Key", "c", "c"));
            a.notifyListeners(i(20, 21), i(), i());
        });

        final Table ex3a = newTable(longCol("ID", 4, 4, 5, 5, 5, 5),
                intCol("Sentinel", 107, 108, 109, 110, 111, 112), col("Key", "a", "a", "b", "b", "c", "c"));
        final Table ex3b = newTable(longCol("Ego", 4, 4, 5, 5), intCol("Sentinel", 205, 206, 207, 208),
                col("Klyuch", "a", "a", "b", "c"));

        showWithRowSet(fa);
        showWithRowSet(fb);

        assertTableEquals(fa, ex3a);
        assertTableEquals(fb, ex3b);


        System.out.println("A before modfications.");
        showWithRowSet(a, 30);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(20, 21), longCol("ID", 5, 5), intCol("Sentinel", 113, 114), col("Key", "c", "c"));
            a.notifyListeners(i(), i(), i(20, 21));
        });

        final Table ex4a = newTable(longCol("ID", 4, 4, 5, 5, 5, 5),
                intCol("Sentinel", 107, 108, 109, 110, 113, 114), col("Key", "a", "a", "b", "b", "c", "c"));
        assertTableEquals(fa, ex4a);
    }

    public void testErrorPropagation() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3, 4, 4, 5, 5),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106, 107, 108, 109, 110),
                col("Key", "a", "a", "b", "b", "a", "a", "a", "a", "b", "b"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("Ego", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Klyuch", "a", "a", "b", "b", "a", "a"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder();
        builder.addTable("a", a, "ID", "Key");
        builder.addTable("b", b, "Ego", "Klyuch");
        final Map<String, Table> result =
                ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(builder::build);

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        ((QueryTable) fa).setAttribute("NAME", "a");
        ((QueryTable) fb).setAttribute("NAME", "b");

        final ErrorListener la = new ErrorListener("fa", fa);
        final ErrorListener lb = new ErrorListener("fb", fb);

        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
        allowingError(() -> {
            a.getRowSet().writableCast().remove(1);
            a.notifyListeners(i(), i(1), i());
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
        }, throwables -> {
            TestCase.assertEquals(1, getUpdateErrors().size());
            final Throwable throwable = throwables.get(0);
            TestCase.assertEquals(IllegalStateException.class, throwable.getClass());
            TestCase.assertEquals("Can not process removed rows in SyncTableFilter!", throwable.getMessage());
            return true;
        });

        assertNotNull(la.originalException);
        assertNotNull(lb.originalException);
        assertEquals("Can not process removed rows in SyncTableFilter!", la.originalException.getMessage());
        assertEquals("Can not process removed rows in SyncTableFilter!", lb.originalException.getMessage());
    }

    public void testDependencies() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1), intCol("Sentinel", 101), col("Key", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("Ego", 0, 1, 1), intCol("Sentinel", 201, 202, 203),
                col("Klyuch", "a", "a", "b"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder();
        builder.addTable("a", a, "ID");
        builder.addTable("b", b, "Ego");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Map<String, Table> result =
                updateGraph.sharedLock().computeLocked(builder::build);

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        ((QueryTable) fa).setAttribute("NAME", "a");
        ((QueryTable) fb).setAttribute("NAME", "b");


        final Table fau = updateGraph.sharedLock().computeLocked(
                () -> fa.update("SentinelDoubled=Sentinel*2"));
        final Table fbu = updateGraph.sharedLock().computeLocked(
                () -> fb.update("SentinelDoubled=Sentinel*2"));
        final Table joined = updateGraph.sharedLock().computeLocked(
                () -> fau.naturalJoin(fbu, "Key=Klyuch", "SB=Sentinel,SBD=SentinelDoubled"));
        final Table sentSum = updateGraph.sharedLock().computeLocked(
                () -> joined.update("SS=SBD+SentinelDoubled"));

        showWithRowSet(sentSum);

        updateGraph.startCycleForUnitTests();
        assertTrue(sentSum.satisfied(updateGraph.clock().currentStep()));
        updateGraph.completeCycleForUnitTests();

        updateGraph.startCycleForUnitTests();
        addToTable(a, i(1), longCol("ID", 1), intCol("Sentinel", 102), col("Key", "b"));
        a.notifyListeners(i(1), i(), i());
        assertFalse(fa.satisfied(updateGraph.clock().currentStep()));
        assertFalse(fb.satisfied(updateGraph.clock().currentStep()));
        assertFalse(sentSum.satisfied(updateGraph.clock().currentStep()));

        while (!fa.satisfied(updateGraph.clock().currentStep())) {
            updateGraph
                    .flushOneNotificationForUnitTests();
        }
        assertTrue(fa.satisfied(updateGraph.clock().currentStep()));
        updateGraph.flushOneNotificationForUnitTests();
        assertTrue(fb.satisfied(updateGraph.clock().currentStep()));

        assertFalse(joined.satisfied(updateGraph.clock().currentStep()));

        updateGraph.completeCycleForUnitTests();

        showWithRowSet(sentSum);
        int[] actual = (int[]) DataAccessHelpers.getColumn(sentSum, "SS").getDirect();
        int[] expected = new int[] {606, 610};
        assertEquals(expected, actual);
    }

    private static class ErrorListener extends ShiftObliviousInstrumentedListenerAdapter {
        Throwable originalException;

        ErrorListener(String description, Table table) {
            super("Error Checker: " + description, table, false);
            table.addUpdateListener(this, false);
        }

        @Override
        public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
            fail("Should not have gotten an update!");
        }

        @Override
        public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
            this.originalException = originalException;
        }
    }
}
