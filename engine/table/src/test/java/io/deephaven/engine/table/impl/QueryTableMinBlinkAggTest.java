//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import org.junit.Test;

public class QueryTableMinBlinkAggTest extends QueryTableMinAggTestBase {

    @Override
    public Table min(char[] source) {
        final Table x = TableTools.newTable(TableTools.charCol(S1, source));
        x.setRefreshing(true);
        ((QueryTable) x).setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
        return x.minBy();
    }

    @Override
    public Table min(float[] source) {
        final Table x = TableTools.newTable(TableTools.floatCol(S1, source));
        x.setRefreshing(true);
        ((QueryTable) x).setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
        return x.minBy();
    }

    @Override
    public Table min(double[] source) {
        final Table x = TableTools.newTable(TableTools.doubleCol(S1, source));
        x.setRefreshing(true);
        ((QueryTable) x).setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
        return x.minBy();
    }
}
