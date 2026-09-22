//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.engine.exceptions.DuplicateRightKeyException;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.*;

/**
 * Natural joins keyed on float and double columns holding NaN.
 *
 * <p>
 * The engine's key equality comes from {@link io.deephaven.util.compare.FloatComparisons#eq} and
 * {@link io.deephaven.util.compare.DoubleComparisons#eq}, so NaN is its own key and matches NaN, -0.0 and 0.0 are the
 * same key, and the null value is a key like any other. {@link io.deephaven.engine.util.TableDiff} compares through the
 * same equality, so the key column can be asserted directly.
 * </p>
 */
@Category(OutOfBandTest.class)
public class QueryTableNaturalJoinNanKeyTest extends QueryTableTestBase {

    public void testStaticFloatNaNKeys() {
        final Table left = testTable(floatCol("K", Float.NaN, 1.5f, -0.0f, NULL_FLOAT), intCol("L", 1, 2, 3, 4));
        final Table right = testTable(floatCol("K", Float.NaN, 1.5f, 0.0f, NULL_FLOAT), intCol("R", 10, 20, 30, 40));

        assertTableEquals(
                newTable(floatCol("K", Float.NaN, 1.5f, -0.0f, NULL_FLOAT), intCol("L", 1, 2, 3, 4),
                        intCol("R", 10, 20, 30, 40)),
                left.naturalJoin(right, "K", "R"));
    }

    public void testStaticDoubleNaNKeys() {
        final Table left = testTable(doubleCol("K", Double.NaN, 1.5, -0.0, NULL_DOUBLE), intCol("L", 1, 2, 3, 4));
        final Table right = testTable(doubleCol("K", Double.NaN, 1.5, 0.0, NULL_DOUBLE), intCol("R", 10, 20, 30, 40));

        assertTableEquals(
                newTable(doubleCol("K", Double.NaN, 1.5, -0.0, NULL_DOUBLE), intCol("L", 1, 2, 3, 4),
                        intCol("R", 10, 20, 30, 40)),
                left.naturalJoin(right, "K", "R"));
    }

    /** A NaN left key with no NaN on the right takes the unmatched null, rather than matching some other value. */
    public void testStaticNaNKeyUnmatched() {
        final Table left = testTable(doubleCol("K", Double.NaN, 1.5), intCol("L", 1, 2));
        final Table right = testTable(doubleCol("K", 1.5, 2.5), intCol("R", 20, 25));

        assertTableEquals(
                newTable(doubleCol("K", Double.NaN, 1.5), intCol("L", 1, 2), intCol("R", NULL_INT, 20)),
                left.naturalJoin(right, "K", "R"));
    }

    /** Duplicate NaN right keys are duplicates of one key, so FIRST_MATCH and LAST_MATCH select between them. */
    public void testStaticNaNDuplicateRightFirstAndLastMatch() {
        final Table left = testTable(doubleCol("K", Double.NaN), intCol("L", 1));
        final Table right = testTable(doubleCol("K", Double.NaN, Double.NaN), intCol("R", 10, 11));

        assertTableEquals(newTable(doubleCol("K", Double.NaN), intCol("L", 1), intCol("R", 10)),
                left.naturalJoin(right, "K", "R", NaturalJoinType.FIRST_MATCH));
        assertTableEquals(newTable(doubleCol("K", Double.NaN), intCol("L", 1), intCol("R", 11)),
                left.naturalJoin(right, "K", "R", NaturalJoinType.LAST_MATCH));
    }

    /** Duplicate NaN right keys are reported like any other duplicate key. */
    public void testStaticNaNDuplicateRightErrors() {
        final Table left = testTable(doubleCol("K", Double.NaN), intCol("L", 1));
        final Table right = testTable(doubleCol("K", Double.NaN, Double.NaN), intCol("R", 10, 11));

        final DuplicateRightKeyException err = Assert.assertThrows(DuplicateRightKeyException.class,
                () -> left.naturalJoin(right, "K", "R"));
        assertTrue(err.getMessage(), err.getMessage().startsWith("Natural Join found duplicate right key for "));
    }

    public void testRightRefreshingFloatNaNKeys() {
        final QueryTable right = testRefreshingTable(i(0, 1).toTracking(),
                floatCol("K", 1.5f, -0.0f), intCol("R", 20, 30));
        final Table left = testTable(floatCol("K", Float.NaN, 1.5f, 0.0f), intCol("L", 1, 2, 3));
        final Table result = left.naturalJoin(right, "K", "R");

        assertTableEquals(newTable(floatCol("K", Float.NaN, 1.5f, 0.0f), intCol("L", 1, 2, 3),
                intCol("R", NULL_INT, 20, 30)), result);

        // a NaN right row arrives and claims the NaN left key
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(2), floatCol("K", Float.NaN), intCol("R", 10));
            right.notifyListeners(i(2), i(), i());
        });
        assertTableEquals(newTable(floatCol("K", Float.NaN, 1.5f, 0.0f), intCol("L", 1, 2, 3),
                intCol("R", 10, 20, 30)), result);

        // and when it is removed the NaN left key is unmatched again
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(right, i(2));
            right.notifyListeners(i(), i(2), i());
        });
        assertTableEquals(newTable(floatCol("K", Float.NaN, 1.5f, 0.0f), intCol("L", 1, 2, 3),
                intCol("R", NULL_INT, 20, 30)), result);
    }

    public void testBothRefreshingDoubleNaNKeys() {
        final QueryTable left = testRefreshingTable(i(0).toTracking(), doubleCol("K", 1.5), intCol("L", 1));
        final QueryTable right = testRefreshingTable(i(0).toTracking(), doubleCol("K", Double.NaN), intCol("R", 10));
        final Table result = left.naturalJoin(right, "K", "R");

        assertTableEquals(newTable(doubleCol("K", 1.5), intCol("L", 1), intCol("R", NULL_INT)), result);

        // a left row re-keyed to NaN picks up the NaN right row
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(0), doubleCol("K", Double.NaN), intCol("L", 1));
            left.notifyListeners(i(), i(), i(0));
        });
        assertTableEquals(newTable(doubleCol("K", Double.NaN), intCol("L", 1), intCol("R", 10)), result);

        // a NaN left row added alongside it matches the same right row
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(1), doubleCol("K", Double.NaN), intCol("L", 2));
            left.notifyListeners(i(1), i(), i());
        });
        assertTableEquals(newTable(doubleCol("K", Double.NaN, Double.NaN), intCol("L", 1, 2), intCol("R", 10, 10)),
                result);
    }

    /** A randomized incremental join over float and double keys drawn from the interesting values. */
    public void testIncrementalNaNKeysRandomized() {
        for (int seed = 0; seed < 3; ++seed) {
            incrementalNaNKeys(seed, true);
            incrementalNaNKeys(seed, false);
        }
    }

    private void incrementalNaNKeys(final int seed, final boolean useDouble) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] rightInfo;
        final QueryTable right = getTable(true, 40, random, rightInfo = initColumnInfos(new String[] {"K", "R"},
                useDouble
                        ? new SetGenerator<>(Double.class, Double.NaN, 0.0,
                                -0.0, 1.5, NULL_DOUBLE)
                        : new SetGenerator<>(Float.class, Float.NaN, 0.0f,
                                -0.0f, 1.5f, NULL_FLOAT),
                new IntGenerator(0, 1000)));
        final ColumnInfo<?, ?>[] leftInfo;
        final QueryTable left = getTable(true, 60, random, leftInfo = initColumnInfos(new String[] {"K", "L"},
                useDouble
                        ? new SetGenerator<>(Double.class, Double.NaN, 0.0,
                                -0.0, 1.5, NULL_DOUBLE)
                        : new SetGenerator<>(Float.class, Float.NaN, 0.0f,
                                -0.0f, 1.5f, NULL_FLOAT),
                new IntGenerator(0, 1000)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> left.naturalJoin(right, "K", "R", NaturalJoinType.FIRST_MATCH)),
                EvalNugget.from(() -> left.naturalJoin(right, "K", "R", NaturalJoinType.LAST_MATCH)),
        };

        for (int step = 0; step < 6; ++step) {
            simulateShiftAwareStep("seed " + seed + " double=" + useDouble + " step " + step, 10, random, left,
                    leftInfo, en);
            simulateShiftAwareStep("seed " + seed + " double=" + useDouble + " right step " + step, 10, random, right,
                    rightInfo, en);
        }
    }
}
