//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

public class QueryTableSortedFirstStaticAggTest extends QueryTableSortedFirstAggTestBase {
    @Override
    public Table sortedFirst(char[] source) {
        return TableTools.newTable(TableTools.charCol(S1, source)).aggBy(Aggregation.AggSortedFirst(S1, S1));
    }

    @Override
    public Table sortedFirst(float[] source) {
        return TableTools.newTable(TableTools.floatCol(S1, source)).aggBy(Aggregation.AggSortedFirst(S1, S1));
    }

    @Override
    public Table sortedFirst(double[] source) {
        return TableTools.newTable(TableTools.doubleCol(S1, source)).aggBy(Aggregation.AggSortedFirst(S1, S1));
    }
}
