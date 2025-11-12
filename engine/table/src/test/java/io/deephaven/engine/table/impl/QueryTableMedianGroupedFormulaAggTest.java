//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import org.junit.Assume;
import org.junit.Ignore;

@Ignore
public class QueryTableMedianGroupedFormulaAggTest extends QueryTableMedianAggTestBase {

    @Override
    public Table median(char[] source) {
        // Assumptions.assumeThat()
        throw new UnsupportedOperationException();
    }

    @Override
    public Table median(float[] source) {
        // todo: need to fix numeric so median(float) -> float?
        throw new UnsupportedOperationException();
    }

    @Override
    public Table median(double[] source) {
        return TableTools.newTable(TableTools.doubleCol(S1, source)).groupBy().update("S1=median(S1)");
    }

    @Override
    public void testEmptyChar() {}

    @Override
    public void testSplitChar() {}

    @Override
    public void testChar() {}

    @Override
    public void testAllNullChar() {}

    @Override
    public void testSkipsNullChar() {}

    @Override
    public void testEmptyFloat() {}

    @Override
    public void testSplitFloat() {}

    @Override
    public void testFloat() {}

    @Override
    public void testAllNullFloat() {}

    @Override
    public void testSkipsNullFloat() {}

    @Override
    public void testSkipsNanFloat() {}

    @Override
    public void testNegZeroFirstFloat() {}

    @Override
    public void testZeroFirstFloat() {}

    @Override
    public void testAllNaNFloat() {}
}
