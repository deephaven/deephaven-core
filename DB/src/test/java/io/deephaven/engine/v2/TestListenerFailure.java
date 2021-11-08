package io.deephaven.engine.v2;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.live.UpdateGraphProcessor;
import io.deephaven.engine.tables.utils.TableTools;
import io.deephaven.engine.v2.select.FormulaEvaluationException;
import io.deephaven.engine.v2.utils.RowSet;
import junit.framework.TestCase;

import java.util.List;

import static io.deephaven.engine.v2.TstUtils.assertTableEquals;
import static io.deephaven.engine.v2.TstUtils.i;

public class TestListenerFailure extends RefreshingTableTestCase {
    public void testListenerFailure() {
        final QueryTable source = TstUtils.testRefreshingTable(TstUtils.c("Str", "A", "B"));
        final Table updated =
                UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> source.update("UC=Str.toUpperCase()"));

        TableTools.showWithIndex(updated);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(2, 3), TstUtils.c("Str", "C", "D"));
            source.notifyListeners(i(2, 3), i(), i());
        });

        assertFalse(updated.isFailed());

        allowingError(() -> {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, i(4, 5), TstUtils.c("Str", "E", null));
                source.notifyListeners(i(4, 5), i(), i());
            });
            return null;
        }, TestListenerFailure::isNpe);

        assertTrue(updated.isFailed());

        try {
            updated.listenForUpdates(new ErrorListener((QueryTable) updated));
            TestCase.fail("Should not be allowed to listen to failed table");
        } catch (IllegalStateException ise) {
            assertEquals("Can not listen to failed table QueryTable", ise.getMessage());
        }

        try {
            updated
                    .listenForUpdates(
                            new ShiftObliviousInstrumentedListenerAdapter("Dummy", (QueryTable) updated, false) {
                                @Override
                                public void onUpdate(RowSet added, RowSet removed, RowSet modified) {}
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
        final Table filtered = UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> viewed.where("UC=`A`"));

        TableTools.showWithIndex(filtered);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(2, 3), TstUtils.c("Str", "C", "D"));
            source.notifyListeners(i(2, 3), i(), i());
        });

        assertFalse(filtered.isFailed());

        final Table filteredAgain = UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> viewed.where("UC=`A`"));
        assertSame(filtered, filteredAgain);

        allowingError(() -> {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, i(4, 5), TstUtils.c("Str", "E", null));
                source.notifyListeners(i(4, 5), i(), i());
            });
            return null;
        }, TestListenerFailure::isFilterNpe);

        assertTrue(filtered.isFailed());
        assertTrue(filteredAgain.isFailed());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(source, i(5));
            source.notifyListeners(i(), i(5), i());
        });

        final Table filteredYetAgain =
                UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> viewed.where("UC=`A`"));
        assertNotSame(filtered, filteredYetAgain);
        assertFalse(filteredYetAgain.isFailed());
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
