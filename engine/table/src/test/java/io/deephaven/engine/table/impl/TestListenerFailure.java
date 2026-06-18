//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.TableAlreadyFailedException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.select.FormulaEvaluationException;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import junit.framework.TestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.col;

public class TestListenerFailure extends RefreshingTableTestCase {
    public void testListenerFailure() {
        final QueryTable source = TstUtils.testRefreshingTable(col("Str", "A", "B"));
        final Table updated = ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                () -> source.update("UC=Str.toUpperCase()"));

        TableTools.showWithRowSet(updated);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(2, 3), col("Str", "C", "D"));
            source.notifyListeners(i(2, 3), i(), i());
        });

        assertFalse(updated.isFailed());

        allowingError(() -> {
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, i(4, 5), col("Str", "E", null));
                source.notifyListeners(i(4, 5), i(), i());
            });
            return null;
        }, TestListenerFailure::isNpe);

        assertTrue(updated.isFailed());

        try {
            updated.addUpdateListener(new ErrorListener(updated));
            TestCase.fail("Should not be allowed to listen to failed table");
        } catch (TableAlreadyFailedException tafe) {
            assertEquals("Can not listen to failed table QueryTable", tafe.getMessage());
        }

        try {
            updated.addUpdateListener(
                    new ShiftObliviousInstrumentedListenerAdapter("Dummy", updated, false) {
                        @Override
                        public void onUpdate(RowSet added, RowSet removed, RowSet modified) {}
                    }, false);
            TestCase.fail("Should not be allowed to listen to failed table");
        } catch (TableAlreadyFailedException tafe) {
            assertEquals("Can not listen to failed table QueryTable", tafe.getMessage());
        }
    }

    private static boolean isNpe(List<Throwable> throwables) {
        if (1 != throwables.size()) {
            return false;
        }
        if (!throwables.get(0).getClass().equals(FormulaEvaluationException.class)) {
            return false;
        }
        if (!throwables.get(0).getMessage().equals("In formula: UC = Str.toUpperCase()")) {
            return false;
        }
        return throwables.get(0).getCause().getClass().equals(NullPointerException.class);
    }

    /**
     * Verify that a leaf {@link InstrumentedTableUpdateListenerAdapter} captures its systemic state at construction
     * time and that {@link io.deephaven.engine.util.systemicmarking.SystemicObject#markSystemic} mutates it.
     */
    public void testLeafListenerSystemicMarking() {
        // dh-tests.prop sets SystemicObjectTracker.enabled=true; without it there is nothing to differentiate.
        assertTrue("Systemic object marking must be enabled for this test",
                SystemicObjectTracker.isSystemicObjectMarkingEnabled());
        final QueryTable source = TstUtils.testRefreshingTable(col("Str", "A", "B"));

        final InstrumentedTableUpdateListenerAdapter systemic = SystemicObjectTracker.executeSystemically(true,
                () -> new InstrumentedTableUpdateListenerAdapter("Systemic", source, false) {
                    @Override
                    public void onUpdate(final TableUpdate upstream) {}
                });
        assertTrue(systemic.isSystemicObject());

        final InstrumentedTableUpdateListenerAdapter nonSystemic = SystemicObjectTracker.executeSystemically(false,
                () -> new InstrumentedTableUpdateListenerAdapter("NonSystemic", source, false) {
                    @Override
                    public void onUpdate(final TableUpdate upstream) {}
                });
        assertFalse(nonSystemic.isSystemicObject());

        // markSystemic returns this and flips the flag
        assertSame(nonSystemic, nonSystemic.markSystemic());
        assertTrue(nonSystemic.isSystemicObject());
    }

    /**
     * A systemic leaf listener that fails should deliver a secondary client error notification, while a non-systemic
     * one should not. The error is always logged regardless.
     */
    public void testLeafListenerFailureNotifiesWhenSystemic() {
        // dh-tests.prop sets SystemicObjectTracker.enabled=true; without it every object is systemic and a
        // non-systemic listener could not be distinguished.
        assertTrue("Systemic object marking must be enabled for this test",
                SystemicObjectTracker.isSystemicObjectMarkingEnabled());

        // A systemic listener always reports.
        assertEquals(1, reportsFromFailingUpdateListener(true, false));
        assertEquals(1, reportsFromFailingShiftObliviousListener(true, false));

        // A non-systemic listener reports only its log, not the secondary client error notification.
        assertEquals(0, reportsFromFailingUpdateListener(false, false));
        assertEquals(0, reportsFromFailingShiftObliviousListener(false, false));

        // ... unless it has been explicitly marked systemic after construction.
        assertEquals(1, reportsFromFailingUpdateListener(false, true));
        assertEquals(1, reportsFromFailingShiftObliviousListener(false, true));
    }

    private static int reportsFromFailingUpdateListener(final boolean constructSystemic, final boolean markAfter) {
        final QueryTable source = TstUtils.testRefreshingTable(col("Str", "A", "B"));
        final InstrumentedTableUpdateListenerAdapter listener = SystemicObjectTracker.executeSystemically(
                constructSystemic,
                () -> new InstrumentedTableUpdateListenerAdapter("Failing Update Listener", source, false) {
                    @Override
                    public void onUpdate(final TableUpdate upstream) {
                        throw new RuntimeException("Test-induced update listener failure");
                    }
                });
        if (markAfter) {
            listener.markSystemic();
        }
        source.addUpdateListener(listener);
        return countReportsFromFailingCycle(source);
    }

    private static int reportsFromFailingShiftObliviousListener(
            final boolean constructSystemic, final boolean markAfter) {
        final QueryTable source = TstUtils.testRefreshingTable(col("Str", "A", "B"));
        final ShiftObliviousInstrumentedListenerAdapter listener = SystemicObjectTracker.executeSystemically(
                constructSystemic,
                () -> new ShiftObliviousInstrumentedListenerAdapter("Failing ShiftOblivious Listener", source, false) {
                    @Override
                    public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
                        throw new RuntimeException("Test-induced shift-oblivious listener failure");
                    }
                });
        if (markAfter) {
            listener.markSystemic();
        }
        source.addUpdateListener(listener, false);
        return countReportsFromFailingCycle(source);
    }

    /**
     * Install a counting {@link UpdateErrorReporter}, run a cycle that drives {@code source} to fail its listener, and
     * return the number of secondary client error notifications that were delivered.
     */
    private static int countReportsFromFailingCycle(final QueryTable source) {
        final AtomicInteger reportCount = new AtomicInteger();
        final UpdateErrorReporter oldReporter =
                AsyncClientErrorNotifier.setReporter(t -> reportCount.incrementAndGet());
        try {
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, i(3), col("Str", "C"));
                source.notifyListeners(i(3), i(), i());
            });
        } finally {
            AsyncClientErrorNotifier.setReporter(oldReporter);
        }
        return reportCount.get();
    }

    public void testMemoCheck() {
        QueryTable.setMemoizeResults(true);

        final QueryTable source = TstUtils.testRefreshingTable(col("Str", "A", "B"));
        final QueryTable viewed = (QueryTable) source.updateView("UC=Str.toUpperCase()");
        final Table filtered = viewed.where("UC=`A`");

        TableTools.showWithRowSet(filtered);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(2, 3), col("Str", "C", "D"));
            source.notifyListeners(i(2, 3), i(), i());
        });

        assertFalse(filtered.isFailed());

        final Table filteredAgain = viewed.where("UC=`A`");
        assertSame(filtered, filteredAgain);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(4, 5), col("Str", "E", null));
            source.notifyListeners(i(4, 5), i(), i());
        });

        assertTrue(filtered.isFailed());
        assertTrue(filteredAgain.isFailed());

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(source, i(5));
            source.notifyListeners(i(), i(5), i());
        });

        final Table filteredYetAgain = viewed.where("UC=`A`");
        assertNotSame(filtered, filteredYetAgain);
        assertFalse(filteredYetAgain.isFailed());
        assertTableEquals(TableTools.newTable(col("Str", "A"), col("UC", "A")), filteredYetAgain);
    }
}
