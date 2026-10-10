//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.TableAlreadyFailedException;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.MultiJoinFactory;
import io.deephaven.engine.table.MultiJoinInput;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.OuterJoinTools;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * An operation over several inputs, one of which has already failed, throws without leaving a listener on any of its
 * healthy inputs.
 */
public class QueryTableJoinFailedInputTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static QueryTable makeLeft() {
        return testRefreshingTable(i(2).toTracking(), intCol("K", 1), intCol("S", 1), intCol("LV", 10));
    }

    private static QueryTable makeRight() {
        return testRefreshingTable(i(3).toTracking(), intCol("K", 1), intCol("S", 1), intCol("RV", 100));
    }

    private static void markFailed(final QueryTable table) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(
                () -> table.notifyListenersOnError(new RuntimeException("simulated upstream failure"), null));
        assertTrue(table.isFailed());
    }

    private static void checkFailedInputLeavesNoListeners(final String name, final BinaryOperator<Table> operation) {
        for (final boolean failLeft : new boolean[] {false, true}) {
            final String description = name + (failLeft ? " with a failed left" : " with a failed right");
            final QueryTable left = makeLeft();
            final QueryTable right = makeRight();
            final QueryTable healthy = failLeft ? right : left;
            markFailed(failLeft ? left : right);

            try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(), true)) {
                final RuntimeException failure =
                        assertThrows(description, RuntimeException.class, () -> operation.apply(left, right));
                // an operation that first derives a table from the failed input reports the failure as the cause
                assertTrue(description + ": " + failure,
                        ExceptionUtils.indexOfThrowable(failure, TableAlreadyFailedException.class) >= 0);
                assertFalse(description, healthy.hasListeners());
            }
            assertFalse(description, healthy.hasListeners());
        }
    }

    @Test
    public void testNaturalJoin() {
        checkFailedInputLeavesNoListeners("keyed naturalJoin", (left, right) -> left.naturalJoin(right, "K", "RV"));
        checkFailedInputLeavesNoListeners("zero-key naturalJoin", (left, right) -> left.naturalJoin(right, "", "RV"));
    }

    @Test
    public void testExactJoin() {
        checkFailedInputLeavesNoListeners("keyed exactJoin", (left, right) -> left.exactJoin(right, "K", "RV"));
        checkFailedInputLeavesNoListeners("zero-key exactJoin", (left, right) -> left.exactJoin(right, "", "RV"));
    }

    @Test
    public void testAj() {
        checkFailedInputLeavesNoListeners("keyed aj", (left, right) -> left.aj(right, "K,S", "RV"));
        checkFailedInputLeavesNoListeners("zero-key aj", (left, right) -> left.aj(right, "S", "RV"));
    }

    @Test
    public void testRaj() {
        checkFailedInputLeavesNoListeners("keyed raj", (left, right) -> left.raj(right, "K,S", "RV"));
        checkFailedInputLeavesNoListeners("zero-key raj", (left, right) -> left.raj(right, "S", "RV"));
    }

    @Test
    public void testJoin() {
        checkFailedInputLeavesNoListeners("keyed join", (left, right) -> left.join(right, "K", "RV"));
        checkFailedInputLeavesNoListeners("zero-key join", (left, right) -> left.join(right, "", "RV"));
    }

    @Test
    public void testLeftOuterJoin() {
        checkFailedInputLeavesNoListeners("keyed leftOuterJoin",
                (left, right) -> OuterJoinTools.leftOuterJoin(left, right, "K", "RV"));
        checkFailedInputLeavesNoListeners("zero-key leftOuterJoin",
                (left, right) -> OuterJoinTools.leftOuterJoin(left, right, "", "RV"));
    }

    @Test
    public void testFullOuterJoin() {
        checkFailedInputLeavesNoListeners("keyed fullOuterJoin",
                (left, right) -> OuterJoinTools.fullOuterJoin(left, right, "K", "RV"));
    }

    @Test
    public void testJoinsWithFailedAndStaticInputs() {
        final Map<String, BinaryOperator<Table>> operations = new LinkedHashMap<>();
        operations.put("keyed join", (left, right) -> left.join(right, "K", "RV"));
        operations.put("zero-key join", (left, right) -> left.join(right, "", "RV"));
        operations.put("keyed leftOuterJoin", (left, right) -> OuterJoinTools.leftOuterJoin(left, right, "K", "RV"));
        operations.put("zero-key leftOuterJoin",
                (left, right) -> OuterJoinTools.leftOuterJoin(left, right, "", "RV"));
        operations.put("keyed fullOuterJoin",
                (left, right) -> OuterJoinTools.fullOuterJoin(left, right, "K", "RV"));
        operations.put("keyed naturalJoin", (left, right) -> left.naturalJoin(right, "K", "RV"));
        operations.put("zero-key naturalJoin", (left, right) -> left.naturalJoin(right, "", "RV"));
        operations.put("keyed exactJoin", (left, right) -> left.exactJoin(right, "K", "RV"));
        operations.put("zero-key exactJoin", (left, right) -> left.exactJoin(right, "", "RV"));
        operations.put("keyed aj", (left, right) -> left.aj(right, "K,S", "RV"));
        operations.put("zero-key aj", (left, right) -> left.aj(right, "S", "RV"));
        operations.put("keyed raj", (left, right) -> left.raj(right, "K,S", "RV"));
        operations.put("zero-key raj", (left, right) -> left.raj(right, "S", "RV"));
        operations.put("keyed rangeJoin", (left, right) -> left.rangeJoin(right, List.of("K", "S <= RV <= LV"),
                List.of(Aggregation.AggGroup("RV"))));
        operations.put("zero-key rangeJoin", (left, right) -> left.rangeJoin(right, List.of("S <= RV <= LV"),
                List.of(Aggregation.AggGroup("RV"))));
        final List<String> accepted = new ArrayList<>();
        for (final Map.Entry<String, BinaryOperator<Table>> entry : operations.entrySet()) {
            for (final boolean failLeft : new boolean[] {false, true}) {
                for (final boolean otherEmpty : new boolean[] {false, true}) {
                    // a static input, particularly an empty one, can make a result that never changes
                    final String description = entry.getKey() + (failLeft ? " with a failed left and a static "
                            : " with a failed right and a static ") + (otherEmpty ? "empty " : "")
                            + (failLeft ? "right" : "left");
                    final QueryTable failed = failLeft ? makeLeft() : makeRight();
                    markFailed(failed);
                    final Table staticOther = otherEmpty
                            ? TableTools.newTable(intCol("K"), intCol("S"), intCol(failLeft ? "RV" : "LV"))
                            : TableTools.newTable(intCol("K", 1), intCol("S", 1), intCol(failLeft ? "RV" : "LV", 1));
                    final Table left = failLeft ? failed : staticOther;
                    final Table right = failLeft ? staticOther : failed;
                    try {
                        final Table result = entry.getValue().apply(left, right);
                        accepted.add(description + " returned a " + (result.isRefreshing() ? "refreshing" : "static")
                                + " result");
                    } catch (final RuntimeException failure) {
                        if (ExceptionUtils.indexOfThrowable(failure, TableAlreadyFailedException.class) < 0) {
                            accepted.add(description + " threw " + failure);
                        }
                    }
                }
            }
        }
        assertTrue(String.join("\n", accepted), accepted.isEmpty());
    }

    @Test
    public void testMultiJoin() {
        checkFailedInputLeavesNoListeners("keyed multiJoin", (left, right) -> MultiJoinFactory.of(
                MultiJoinInput.of(left, "K", "LV"), MultiJoinInput.of(right, "K", "RV")).table());
        checkFailedInputLeavesNoListeners("zero-key multiJoin", (left, right) -> MultiJoinFactory.of(
                MultiJoinInput.of(left, "", "LV"), MultiJoinInput.of(right, "", "RV")).table());
    }

    @Test
    public void testWhereIn() {
        checkFailedInputLeavesNoListeners("whereIn", (left, right) -> left.whereIn(right, "K"));
        checkFailedInputLeavesNoListeners("whereNotIn", (left, right) -> left.whereNotIn(right, "K"));
    }
}
