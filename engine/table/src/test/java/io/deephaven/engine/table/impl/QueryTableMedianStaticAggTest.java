//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

public class QueryTableMedianStaticAggTest extends QueryTableMedianAggTestBase {

    @Override
    public Table median(char[] source) {
        return TableTools.newTable(TableTools.charCol(S1, source)).medianBy();
    }

    @Override
    public Table median(float[] source) {
        return TableTools.newTable(TableTools.floatCol(S1, source)).medianBy();
    }

    @Override
    public Table median(double[] source) {
        return TableTools.newTable(TableTools.doubleCol(S1, source)).medianBy();
    }
}
