package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.select.FormulaEvaluationException;
import io.deephaven.db.v2.utils.Index;
import junit.framework.TestCase;

import java.util.List;

import static io.deephaven.db.v2.TstUtils.assertTableEquals;
import static io.deephaven.db.v2.TstUtils.i;

public class TestListenerFailure extends LiveTableTestCase {
    public void testListenerFailure() {
        final QueryTable source = TstUtils.testRefreshingTable(TstUtils.c("Str", "A", "B"));
        final Table updated =
                LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> source.update("UC=Str.toUpperCase()"));

        TableTools.showWithIndex(updated);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(2, 3), TstUtils.c("Str", "C", "D"));
            source.notifyListeners(i(2, 3), i(), i());
        });

        assertFalse(((DynamicTable) updated).isFailed());

        allowingError(() -> {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, i(4, 5), TstUtils.c("Str", "E", null));
                source.notifyListeners(i(4, 5), i(), i());
            });
            return null;
        }, TestListenerFailure::isNpe);

        assertTrue(((DynamicTable) updated).isFailed());

        try {
            ((DynamicTable) updated).listenForUpdates(new ErrorListener((QueryTable) updated));
            TestCase.fail("Should not be allowed to listen to failed table");
        } catch (IllegalStateException ise) {
            assertEquals("Can not listen to failed table QueryTable", ise.getMessage());
        }

        try {
            ((DynamicTable) updated)
                    .listenForUpdates(new InstrumentedListenerAdapter("Dummy", (QueryTable) updated, false) {
                        @Override
                        public void onUpdate(Index added, Index removed, Index modified) {}
                    }, false);
            TestCase.fail("Should not be allowed to listen to failed table");
        } catch (IllegalStateException ise) {
            assertEquals("Can not listen to failed table QueryTable", ise.getMessage());
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

    public void testMemoCheck() {
        QueryTable.setMemoizeResults(true);

        final QueryTable source = TstUtils.testRefreshingTable(TstUtils.c("Str", "A", "B"));
        final QueryTable viewed = (QueryTable) source.updateView("UC=Str.toUpperCase()");
        final Table filtered = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> viewed.where("UC=`A`"));

        TableTools.showWithIndex(filtered);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(2, 3), TstUtils.c("Str", "C", "D"));
            source.notifyListeners(i(2, 3), i(), i());
        });

        assertFalse(((DynamicTable) filtered).isFailed());

        final Table filteredAgain = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> viewed.where("UC=`A`"));
        assertSame(filtered, filteredAgain);

        allowingError(() -> {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, i(4, 5), TstUtils.c("Str", "E", null));
                source.notifyListeners(i(4, 5), i(), i());
            });
            return null;
        }, TestListenerFailure::isFilterNpe);

        assertTrue(((DynamicTable) filtered).isFailed());
        assertTrue(((DynamicTable) filteredAgain).isFailed());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(source, i(5));
            source.notifyListeners(i(), i(5), i());
        });

        final Table filteredYetAgain =
                LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> viewed.where("UC=`A`"));
        assertNotSame(filtered, filteredYetAgain);
        assertFalse(((DynamicTable) filteredYetAgain).isFailed());
        assertTableEquals(TableTools.newTable(TableTools.col("Str", "A"), TableTools.col("UC", "A")), filteredYetAgain);
    }

    private static boolean isFilterNpe(List<Throwable> throwables) {
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
}
