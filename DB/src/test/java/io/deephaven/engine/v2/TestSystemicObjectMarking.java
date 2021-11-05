package io.deephaven.engine.v2;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.tables.utils.SystemicObjectTracker;
import io.deephaven.engine.tables.utils.TableTools;
import io.deephaven.engine.v2.select.FormulaEvaluationException;
import junit.framework.TestCase;

import java.util.List;

import static io.deephaven.engine.v2.TstUtils.c;
import static io.deephaven.engine.v2.TstUtils.i;

public class TestSystemicObjectMarking extends LiveTableTestCase {
    public void testSystemicObjectMarking() {
        final QueryTable source = TstUtils.testRefreshingTable(c("Str", "a", "b"), c("Str2", "A", "B"));
        final Table updated = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> source.update("UC=Str.toUpperCase()"));
        final Table updated2 = SystemicObjectTracker.executeSystemically(false, () -> LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> source.update("LC=Str2.toLowerCase()")));

        TableTools.showWithIndex(updated);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(2, 3), c("Str", "c", "d"), c("Str2", "C", "D"));
            source.notifyListeners(i(2, 3), i(), i());
        });

        assertFalse(updated.isFailed());
        assertFalse(updated2.isFailed());

        final ErrorListener errorListener2 = new ErrorListener((QueryTable)updated2);
        ((QueryTable) updated2).listenForUpdates(errorListener2);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(4, 5), c("Str", "e", "f"), c("Str2", "E", null));
            source.notifyListeners(i(4, 5), i(), i());
        });

        assertFalse(updated.isFailed());
        assertTrue(updated2.isFailed());
        assertNotNull(errorListener2.originalException);
        assertEquals("In formula: LC = Str2.toLowerCase()", errorListener2.originalException.getMessage());

        try {
            updated2.listenForUpdates(new ErrorListener(updated2));
            TestCase.fail("Should not be allowed to listen to failed table");
        } catch (IllegalStateException ise) {
            assertEquals("Can not listen to failed table QueryTable", ise.getMessage());
        }

        final ErrorListener errorListener = new ErrorListener((QueryTable)updated);
        ((QueryTable) updated).listenForUpdates(errorListener);

        allowingError(() -> {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(source, i(7, 8), c("Str", "g", null), c("Str2", "G", "H"));
                source.notifyListeners(i(7, 8), i(), i());
            });
            return null;
        }, TestSystemicObjectMarking::isNpe);

        assertTrue(updated.isFailed());
        assertTrue(updated2.isFailed());
        assertNotNull(errorListener.originalException);
        assertEquals("In formula: UC = Str.toUpperCase()", errorListener.originalException.getMessage());

    }


    private static boolean isNpe(List<Throwable> throwables) {
        if (1 !=  throwables.size()) {
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
