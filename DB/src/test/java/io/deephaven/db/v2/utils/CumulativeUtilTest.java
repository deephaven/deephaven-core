/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.tables.Table;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.tables.utils.TableTools;

public class CumulativeUtilTest extends BaseArrayTestCase {

    public void testCumSum() {
        final Table t = TableTools.emptyTable(10).updateView("Row = i");
        final String cumSum = "CumSum";

        Table t2 = CumulativeUtil.cumSum(t, cumSum, "Row");
        assertColumnEquals(t2, cumSum, new double[] {0, 1, 3, 6, 10, 15, 21, 28, 36, 45});

        t2 = CumulativeUtil.cumSum(t, cumSum, "i % 2 == 0 ? Row : 0");
        assertColumnEquals(t2, cumSum, new double[] {0, 0, 2, 2, 6, 6, 12, 12, 20, 20});


        t2 = CumulativeUtil.cumSum(t.updateView("Key = 45"), "Key", cumSum, "Row");
        assertColumnEquals(t2, cumSum, new double[] {0, 1, 3, 6, 10, 15, 21, 28, 36, 45});
    }

    public void testCumMin() {
        final Table t = TableTools.emptyTable(10).updateView("Row = i");
        final String cumMin = "CumMin";

        Table t2 = CumulativeUtil.cumMin(t, cumMin, "Row");
        assertColumnEquals(t2, cumMin, new double[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

        t2 = CumulativeUtil.cumMin(t, cumMin, "-1*Row");
        assertColumnEquals(t2, cumMin, new double[] {0, -1, -2, -3, -4, -5, -6, -7, -8, -9});
    }

    public void testRollingSum() {
        final Table t = TableTools.emptyTable(10).updateView("Row = i");

        String rollingSum = "RollingSum";
        Table t2 = CumulativeUtil.rollingSum(t, 5, rollingSum, "Row");
        assertColumnEquals(t2, rollingSum,
                new double[] {QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, 15, 20, 25, 30, 35});

        t2 = CumulativeUtil.rollingSum(t, 3, rollingSum, "Row * 2");
        assertColumnEquals(t2, rollingSum, new double[] {QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                QueryConstants.NULL_DOUBLE, 12, 18, 24, 30, 36, 42, 48});
    }

    private void assertColumnEquals(final Table t, final String column, final double[] values) {
        final double[] colValues = t.getColumn(column).getDoubles(0, t.size());
        assertEquals(values, colValues);
    }
}
