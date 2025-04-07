//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import org.junit.Test;

public class QueryTableMedianRefreshingAggTest extends QueryTableMedianAggTestBase {

    @Override
    public Table median(char[] source) {
        final Table x = TableTools.newTable(TableTools.charCol(S1, source));
        x.setRefreshing(true);
        return x.medianBy();
    }

    @Override
    public Table median(float[] source) {
        final Table x = TableTools.newTable(TableTools.floatCol(S1, source));
        x.setRefreshing(true);
        return x.medianBy();
    }

    @Override
    public Table median(double[] source) {
        final Table x = TableTools.newTable(TableTools.doubleCol(S1, source));
        x.setRefreshing(true);
        return x.medianBy();
    }
}
