//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

public class QueryTableMedianViaPercentileBlinkAggTest extends QueryTableMedianAggTestBase {

    @Override
    public Table median(char[] source) {
        final Table x = TableTools.newTable(TableTools.charCol(S1, source));
        x.setRefreshing(true);
        ((QueryTable) x).setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
        return x.aggAllBy(AggSpec.percentile(0.5, true));
    }

    @Override
    public Table median(float[] source) {
        final Table x = TableTools.newTable(TableTools.floatCol(S1, source));
        x.setRefreshing(true);
        ((QueryTable) x).setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
        return x.aggAllBy(AggSpec.percentile(0.5, true));
    }

    @Override
    public Table median(double[] source) {
        final Table x = TableTools.newTable(TableTools.doubleCol(S1, source));
        x.setRefreshing(true);
        ((QueryTable) x).setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
        return x.aggAllBy(AggSpec.percentile(0.5, true));
    }
}
