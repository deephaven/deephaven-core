//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.api.Selectable;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.by.AggregationOperatorException;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.*;
import junit.framework.TestCase;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

public class TestFreezeBy extends RefreshingTableTestCase {
    public void testSimpleTypes() {
        final Instant timeBase = DateTimeUtils.parseInstant("2020-09-10T09:00:00 NY");
        QueryScope.addParam("freezeByTimeBase", timeBase);
        final QueryTable input =
                TstUtils.testRefreshingTable(stringCol("Key", "A", "B", "C"), intCol("Sentinel", 1, 2, 3));
        final List<String> updates = Arrays.asList("SStr=Integer.toString(Sentinel)", "SByte=(byte)Sentinel",
                "SChar=(char)('A' + (char)Sentinel)", "SShort=(short)Sentinel", "SLong=(long)Sentinel",
                "SDouble=Sentinel/4", "SFloat=(float)(Sentinel/2)",
                "SInstant=freezeByTimeBase + (Sentinel * 3600L*1000000000L)",
                "SBoolean=Sentinel%3==0?true:(Sentinel%3==1?false:null)");
        final Table inputUpdated = input.updateView(Selectable.from(updates));
        final Table frozen = FreezeBy.freezeBy(inputUpdated, "Key");
        showWithRowSet(frozen);

        final TableUpdateValidator tuv = TableUpdateValidator.make("frozen", (QueryTable) frozen);
        final FailureListener failureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(failureListener);

        assertTableEquals(inputUpdated, frozen);
        assertEquals(String.class, frozen.getDefinition().getColumn("SStr").getDataType());
        assertEquals(byte.class, frozen.getDefinition().getColumn("SByte").getDataType());
        assertEquals(short.class, frozen.getDefinition().getColumn("SShort").getDataType());
        assertEquals(char.class, frozen.getDefinition().getColumn("SChar").getDataType());
        assertEquals(int.class, frozen.getDefinition().getColumn("Sentinel").getDataType());
        assertEquals(long.class, frozen.getDefinition().getColumn("SLong").getDataType());
        assertEquals(float.class, frozen.getDefinition().getColumn("SFloat").getDataType());
        assertEquals(double.class, frozen.getDefinition().getColumn("SDouble").getDataType());
        assertEquals(Instant.class, frozen.getDefinition().getColumn("SInstant").getDataType());
        assertEquals(Boolean.class, frozen.getDefinition().getColumn("SBoolean").getDataType());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(input, i(0));
            TstUtils.addToTable(input, i(2), stringCol("Key", "C"), intCol("Sentinel", 4));
            input.notifyListeners(i(), i(0), i(2));
        });
        showWithRowSet(frozen);

        assertTableEquals(TableTools.newTable(stringCol("Key", "B", "C"), intCol("Sentinel", 2, 3))
                .updateView(Selectable.from(updates)), frozen);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(3, 4), stringCol("Key", "D", "A"), intCol("Sentinel", 5, 6));
            input.notifyListeners(i(3, 4), i(), i());
        });
        showWithRowSet(frozen);

        assertTableEquals(TableTools.newTable(stringCol("Key", "A", "B", "C", "D"), intCol("Sentinel", 6, 2, 3, 5))
                .updateView(Selectable.from(updates)), frozen);

        // swap two keys
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(3, 4), stringCol("Key", "A", "D"), intCol("Sentinel", 7, 8));
            input.notifyListeners(i(), i(), i(4, 3));
        });
        showWithRowSet(frozen);

        assertTableEquals(TableTools.newTable(stringCol("Key", "A", "B", "C", "D"), intCol("Sentinel", 6, 2, 3, 5))
                .updateView(Selectable.from(updates)), frozen);

        QueryScope.addParam("freezeByTimeBase", null);
    }

    public void testCompositeKeys() {
        final QueryTable input = TstUtils.testRefreshingTable(stringCol("Key", "A", "A", "C"),
                intCol("Key2", 101, 102, 103), intCol("Sentinel", 1, 2, 3));
        final Table frozen = FreezeBy.freezeBy(input, "Key", "Key2");
        showWithRowSet(frozen);

        final TableUpdateValidator tuv = TableUpdateValidator.make("frozen", (QueryTable) frozen);
        final FailureListener failureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(failureListener);

        assertTableEquals(input, frozen);

        // swap two keys
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(0, 4), stringCol("Key", "A", "D"), intCol("Key2", 101, 101),
                    intCol("Sentinel", 4, 5));
            input.notifyListeners(i(4), i(), i(0));
        });
        showWithRowSet(frozen);

        assertTableEquals(TableTools.newTable(stringCol("Key", "A", "A", "C", "D"), intCol("Key2", 101, 102, 103, 101),
                intCol("Sentinel", 1, 2, 3, 5)), frozen);
    }

    public void testNoKeys() {
        final QueryTable input = TstUtils.testRefreshingTable(stringCol("Key", "A"), intCol("Sentinel", 1));
        final Table frozen = FreezeBy.freezeBy(input);
        showWithRowSet(frozen);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Table originalExpect = updateGraph.sharedLock().computeLocked(input::snapshot);
        assertTableEquals(input, originalExpect);

        final TableUpdateValidator tuv = TableUpdateValidator.make("frozen", (QueryTable) frozen);
        final FailureListener failureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(failureListener);
        assertTableEquals(input, frozen);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(input, i(0));
            TstUtils.addToTable(input, i(2), stringCol("Key", "C"), intCol("Sentinel", 4));
            input.notifyListeners(i(2), i(0), i());
        });
        showWithRowSet(frozen);
        assertTableEquals(originalExpect, frozen);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(2), stringCol("Key", "D"), intCol("Sentinel", 5));
            input.notifyListeners(i(), i(), i(2));
        });
        showWithRowSet(frozen);
        assertTableEquals(originalExpect, frozen);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(input, i(2));
            input.notifyListeners(i(), i(2), i());
        });
        showWithRowSet(frozen);
        assertTableEquals(originalExpect.head(0), frozen);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(2), stringCol("Key", "E"), intCol("Sentinel", 6));
            input.notifyListeners(i(2), i(), i());
        });
        showWithRowSet(frozen);
        final Table newExpect = updateGraph.sharedLock().computeLocked(input::snapshot);
        assertTableEquals(input, newExpect);
        assertTableEquals(newExpect, frozen);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(input, i(3), stringCol("Key", "F"), intCol("Sentinel", 7));
            TstUtils.removeRows(input, i(2));
            input.notifyListeners(i(3), i(2), i());
        });
        assertTableEquals(newExpect, frozen);

        updateGraph.runWithinUnitTestCycle(() -> {
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
        } catch (AggregationOperatorException aoe) {
            assertTrue(aoe.getCause() instanceof IllegalStateException);
            assertEquals("FreezeBy only allows one row per state!", aoe.getCause().getMessage());
        }

        final Table frozen = FreezeBy.freezeBy(input, "Key");
        assertTableEquals(input, frozen);
        allowingError(() -> {
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(input, i(3), stringCol("Key", "A"), intCol("Sentinel", 4));
                input.notifyListeners(i(3), i(), i());
            });
        }, exs -> {
            if (exs.size() != 1) {
                return false;
            }
            final Throwable ex = exs.get(0);
            if (ex instanceof AggregationOperatorException && ex.getCause() instanceof IllegalStateException) {
                return "FreezeBy only allows one row per state!".equals(ex.getCause().getMessage());
            }
            return false;
        });

        try {
            FreezeBy.freezeBy(input, "Key");
            TestCase.fail("Expected exception.");
        } catch (AggregationOperatorException aoe) {
            assertTrue(aoe.getCause() instanceof IllegalStateException);
            assertEquals("FreezeBy only allows one row per state!", aoe.getCause().getMessage());
        }
    }
}
