//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;

public class PercentileAveragesTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    /**
     * Averaging the two middle values does not overflow, for static tables or for refreshing ones.
     */
    @Test
    public void testAverageDoesNotOverflow() {
        final float lowestFloat = Math.nextUp(-Float.MAX_VALUE);
        final double lowestDouble = Math.nextUp(-Double.MAX_VALUE);
        checkAverages(
                newTable(intCol("I", Integer.MAX_VALUE, Integer.MAX_VALUE - 2),
                        longCol("L", Long.MAX_VALUE, Long.MAX_VALUE - 2),
                        floatCol("F", Float.MAX_VALUE, Float.MAX_VALUE),
                        doubleCol("D", Double.MAX_VALUE, Double.MAX_VALUE / 2)),
                newTable(doubleCol("I", Integer.MAX_VALUE - 1),
                        doubleCol("L", (double) (Long.MAX_VALUE - 1)),
                        floatCol("F", Float.MAX_VALUE),
                        doubleCol("D", Double.MAX_VALUE * 0.75)));
        // halving each value before adding would round this average one ULP high
        checkAverages(
                newTable(longCol("L", Long.MAX_VALUE, Long.MAX_VALUE - 1023)),
                newTable(doubleCol("L", 0x1.fffffffffffffp+62)));
        // the minimum integral values and the lowest floating point values are nulls
        checkAverages(
                newTable(intCol("I", Integer.MIN_VALUE + 1, Integer.MIN_VALUE + 3),
                        longCol("L", Long.MIN_VALUE + 1, Long.MIN_VALUE + 3),
                        floatCol("F", lowestFloat, lowestFloat),
                        doubleCol("D", lowestDouble, lowestDouble / 2)),
                newTable(doubleCol("I", Integer.MIN_VALUE + 2),
                        doubleCol("L", (double) (Long.MIN_VALUE + 2)),
                        floatCol("F", lowestFloat),
                        doubleCol("D", lowestDouble * 0.75)));
    }

    /**
     * An infinite value is not halved: averaging it with a finite value gives the infinity, and averaging the two
     * infinities gives NaN.
     */
    @Test
    public void testAverageOfInfinities() {
        checkAverages(
                newTable(floatCol("F", 1.0f, Float.POSITIVE_INFINITY),
                        doubleCol("D", 1.0, Double.POSITIVE_INFINITY)),
                newTable(floatCol("F", Float.POSITIVE_INFINITY),
                        doubleCol("D", Double.POSITIVE_INFINITY)));
        checkAverages(
                newTable(floatCol("F", Float.NEGATIVE_INFINITY, 1.0f),
                        doubleCol("D", Double.NEGATIVE_INFINITY, 1.0)),
                newTable(floatCol("F", Float.NEGATIVE_INFINITY),
                        doubleCol("D", Double.NEGATIVE_INFINITY)));
        checkAverages(
                newTable(floatCol("F", Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY),
                        doubleCol("D", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)),
                newTable(floatCol("F", Float.NaN),
                        doubleCol("D", Double.NaN)));
    }

    private static void checkAverages(final Table data, final Table expected) {
        final QueryTable refreshing = new QueryTable(data.getRowSet().copy().toTracking(), data.getColumnSourceMap());
        refreshing.setRefreshing(true);
        assertTableEquals(expected, data.medianBy());
        assertTableEquals(expected, refreshing.medianBy());
    }
}
