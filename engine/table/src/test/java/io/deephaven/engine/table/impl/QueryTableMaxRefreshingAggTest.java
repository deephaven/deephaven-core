//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import org.junit.Test;

public class QueryTableMaxRefreshingAggTest extends QueryTableMaxAggTestBase {

    @Override
    public Table max(char[] source) {
        final Table x = TableTools.newTable(TableTools.charCol(S1, source));
        x.setRefreshing(true);
        return x.maxBy();
    }

    @Override
    public Table max(float[] source) {
        final Table x = TableTools.newTable(TableTools.floatCol(S1, source));
        x.setRefreshing(true);
        return x.maxBy();
    }

    @Override
    public Table max(double[] source) {
        final Table x = TableTools.newTable(TableTools.doubleCol(S1, source));
        x.setRefreshing(true);
        return x.maxBy();
    }

    // we are _okay_ with refreshing case returning -0.0; it is using sort under the covers, but that is "ok"

    @Test
    public void testZeroFirstFloat() {
        check(-0.0f, new float[] {0.0f, -0.0f});
    }

    @Test
    public void testZeroFirstDouble() {
        check(-0.0, new double[] {0.0, -0.0});
    }
}
