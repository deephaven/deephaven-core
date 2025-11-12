//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

public class QueryTableSortedLastRefreshingAggTest extends QueryTableSortedLastAggTestBase {

    @Override
    public Table sortedLast(char[] source) {
        final Table x = TableTools.newTable(TableTools.charCol(S1, source));
        x.setRefreshing(true);
        return x.aggBy(Aggregation.AggSortedLast(S1, S1));
    }

    @Override
    public Table sortedLast(float[] source) {
        final Table x = TableTools.newTable(TableTools.floatCol(S1, source));
        x.setRefreshing(true);
        return x.aggBy(Aggregation.AggSortedLast(S1, S1));
    }

    @Override
    public Table sortedLast(double[] source) {
        final Table x = TableTools.newTable(TableTools.doubleCol(S1, source));
        x.setRefreshing(true);
        return x.aggBy(Aggregation.AggSortedLast(S1, S1));
    }
}
