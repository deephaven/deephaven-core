package io.deephaven.db.v2.utils;

import io.deephaven.api.Selectable;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.*;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.List;

import static io.deephaven.db.tables.utils.TableTools.intCol;
import static io.deephaven.db.tables.utils.TableTools.stringCol;
import static io.deephaven.db.v2.TstUtils.*;

public class TestFreezeBy extends LiveTableTestCase {
    public void testSimpleTypes() {
        final DBDateTime timeBase = DBTimeUtils.convertDateTime("2020-09-10T09:00:00 NY");
        QueryScope.addParam("freezeByTimeBase", timeBase);
        final QueryTable input =
                TstUtils.testRefreshingTable(stringCol("Key", "A", "B", "C"), intCol("Sentinel", 1, 2, 3));
        final List<String> updates = Arrays.asList("SStr=Integer.toString(Sentinel)", "SByte=(byte)Sentinel",
                "SChar=(char)('A' + (char)Sentinel)", "SShort=(short)Sentinel", "SLong=(long)Sentinel",
                "SDouble=Sentinel/4", "SFloat=(float)(Sentinel/2)",
                "SDateTime=freezeByTimeBase + (Sentinel * 3600L*1000000000L)",
                "SBoolean=Sentinel%3==0?true:(Sentinel%3==1?false:null)");
        final Table inputUpdated = input.updateView(Selectable.from(updates));
        final Table frozen = FreezeBy.freezeBy(inputUpdated, "Key");
        TableTools.showWithIndex(frozen);

        final TableUpdateValidator tuv = TableUpdateValidator.make("frozen", (QueryTable) frozen);
        final FailureListener failureListener = new FailureListener();
        tuv.getResultTable().listenForUpdates(failureListener);

        assertTableEquals(inputUpdated, frozen);
        assertEquals(String.class, frozen.getColumn("SStr").getType());
        assertEquals(byte.class, frozen.getColumn("SByte").getType());
        assertEquals(short.class, frozen.getColumn("SShort").getType());
        assertEquals(char.class, frozen.getColumn("SChar").getType());
        assertEquals(int.class, frozen.getColumn("Sentinel").getType());
        assertEquals(long.class, frozen.getColumn("SLong").getType());
        assertEquals(float.class, frozen.getColumn("SFloat").getType());
        assertEquals(double.class, frozen.getColumn("SDouble").getType());
        assertEquals(DBDateTime.class, frozen.getColumn("SDateTime").getType());
        assertEquals(Boolean.class, frozen.getColumn("SBoolean").getType());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(input, i(0));
            TstUtils.addToTable(input, i(2), stringCol("Key", "C"), intCol("Sentinel", 4));
            input.notifyListeners(i(), i(0), i(2));
        });
        TableTools.showWithIndex(frozen);

        assertTableEquals(TableTools.newTable(stringCol("Key", "B", "C"), intCol("Sentinel", 2, 3))
                .updateView(Selectable.from(updates)), frozen);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(3, 4), stringCol("Key", "D", "A"), intCol("Sentinel", 5, 6));
            input.notifyListeners(i(3, 4), i(), i());
        });
        TableTools.showWithIndex(frozen);

        assertTableEquals(TableTools.newTable(stringCol("Key", "A", "B", "C", "D"), intCol("Sentinel", 6, 2, 3, 5))
                .updateView(Selectable.from(updates)), frozen);

        // swap two keys
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(3, 4), stringCol("Key", "A", "D"), intCol("Sentinel", 7, 8));
            input.notifyListeners(i(), i(), i(4, 3));
        });
        TableTools.showWithIndex(frozen);

        assertTableEquals(TableTools.newTable(stringCol("Key", "A", "B", "C", "D"), intCol("Sentinel", 6, 2, 3, 5))
                .updateView(Selectable.from(updates)), frozen);

        QueryScope.addParam("freezeByTimeBase", null);
    }

    public void testCompositeKeys() {
        final QueryTable input = TstUtils.testRefreshingTable(stringCol("Key", "A", "A", "C"),
                intCol("Key2", 101, 102, 103), intCol("Sentinel", 1, 2, 3));
        final Table frozen = FreezeBy.freezeBy(input, "Key", "Key2");
        TableTools.showWithIndex(frozen);

        final TableUpdateValidator tuv = TableUpdateValidator.make("frozen", (QueryTable) frozen);
        final FailureListener failureListener = new FailureListener();
        tuv.getResultTable().listenForUpdates(failureListener);

        assertTableEquals(input, frozen);

        // swap two keys
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(0, 4), stringCol("Key", "A", "D"), intCol("Key2", 101, 101),
                    intCol("Sentinel", 4, 5));
            input.notifyListeners(i(4), i(), i(0));
        });
        TableTools.showWithIndex(frozen);

        assertTableEquals(TableTools.newTable(stringCol("Key", "A", "A", "C", "D"), intCol("Key2", 101, 102, 103, 101),
                intCol("Sentinel", 1, 2, 3, 5)), frozen);
    }

    public void testNoKeys() {
        final QueryTable input = TstUtils.testRefreshingTable(stringCol("Key", "A"), intCol("Sentinel", 1));
        final Table frozen = FreezeBy.freezeBy(input);
        TableTools.showWithIndex(frozen);

        final Table originalExpect =
                LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> TableTools.emptyTable(1).snapshot(input));
        assertTableEquals(input, originalExpect);

        final TableUpdateValidator tuv = TableUpdateValidator.make("frozen", (QueryTable) frozen);
        final FailureListener failureListener = new FailureListener();
        tuv.getResultTable().listenForUpdates(failureListener);
        assertTableEquals(input, frozen);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(input, i(0));
            TstUtils.addToTable(input, i(2), stringCol("Key", "C"), intCol("Sentinel", 4));
            input.notifyListeners(i(2), i(0), i());
        });
        TableTools.showWithIndex(frozen);
        assertTableEquals(originalExpect, frozen);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(2), stringCol("Key", "D"), intCol("Sentinel", 5));
            input.notifyListeners(i(), i(), i(2));
        });
        TableTools.showWithIndex(frozen);
        assertTableEquals(originalExpect, frozen);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(input, i(2));
            input.notifyListeners(i(), i(2), i());
        });
        TableTools.showWithIndex(frozen);
        assertTableEquals(originalExpect.head(0), frozen);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(2), stringCol("Key", "E"), intCol("Sentinel", 6));
            input.notifyListeners(i(2), i(), i());
        });
        TableTools.showWithIndex(frozen);
        final Table newExpect =
                LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> TableTools.emptyTable(1).snapshot(input));
        assertTableEquals(input, newExpect);
        assertTableEquals(newExpect, frozen);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(3), stringCol("Key", "F"), intCol("Sentinel", 7));
            TstUtils.removeRows(input, i(2));
            input.notifyListeners(i(3), i(2), i());
        });
        assertTableEquals(newExpect, frozen);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(3), stringCol("Key", "G"), intCol("Sentinel", 8));
            input.notifyListeners(i(), i(), i(3));
        });
        assertTableEquals(newExpect, frozen);
    }

    public void testDuplicates() {
        final QueryTable input =
                TstUtils.testRefreshingTable(stringCol("Key", "A", "B", "C"), intCol("Sentinel", 1, 2, 3));
        try {
            FreezeBy.freezeBy(input);
            TestCase.fail("Expected exception.");
        } catch (IllegalStateException ise) {
            assertEquals("FreezeBy only allows one row per state!", ise.getMessage());
        }

        final Table frozen = FreezeBy.freezeBy(input, "Key");
        assertTableEquals(input, frozen);
        allowingError(() -> {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                addToTable(input, i(3), stringCol("Key", "A"), intCol("Sentinel", 4));
                input.notifyListeners(i(3), i(), i());
            });
        }, exs -> {
            if (exs.size() != 1) {
                return false;
            }
            final Throwable ex = exs.get(0);
            if (ex instanceof IllegalStateException) {
                return "FreezeBy only allows one row per state!".equals(ex.getMessage());
            }
            return false;
        });

        try {
            FreezeBy.freezeBy(input, "Key");
            TestCase.fail("Expected exception.");
        } catch (IllegalStateException ise) {
            assertEquals("FreezeBy only allows one row per state!", ise.getMessage());
        }
    }
}
