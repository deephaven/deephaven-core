//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import org.junit.Ignore;
import org.junit.Test;

public class QueryTableMinGroupedFormulaAggTest extends QueryTableMinAggTestBase {

    @Override
    public Table min(char[] source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table min(float[] source) {
        return TableTools.newTable(TableTools.floatCol(S1, source)).groupBy().update("S1=min(S1)");
    }

    @Override
    public Table min(double[] source) {
        return TableTools.newTable(TableTools.doubleCol(S1, source)).groupBy().update("S1=min(S1)");
    }

    @Override
    public void testEmptyChar() {
        // not supported
    }

    @Override
    public void testAllNullChar() {
        // not supported
    }

    @Override
    public void testSkipsNullChar() {
        // not supported
    }
}
