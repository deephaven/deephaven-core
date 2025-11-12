//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import org.junit.Test;

public class QueryTableMinStaticAggTest extends QueryTableMinAggTestBase {

    @Override
    public Table min(char[] source) {
        return TableTools.newTable(TableTools.charCol(S1, source)).minBy();
    }

    @Override
    public Table min(float[] source) {
        return TableTools.newTable(TableTools.floatCol(S1, source)).minBy();
    }

    @Override
    public Table min(double[] source) {
        return TableTools.newTable(TableTools.doubleCol(S1, source)).minBy();
    }
}
