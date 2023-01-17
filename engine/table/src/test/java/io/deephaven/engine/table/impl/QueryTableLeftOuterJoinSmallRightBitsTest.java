package io.deephaven.engine.table.impl;

import org.apache.commons.lang3.mutable.MutableInt;

public class QueryTableLeftOuterJoinSmallRightBitsTest extends QueryTableLeftOuterJoinTestBase {
    public QueryTableLeftOuterJoinSmallRightBitsTest() {
        super(1);
    }

    public void testIncrementalWithKeyColumnsShallow() {
        final int size = 10;

        for (int seed = 0; seed < 100; ++seed) {
            testIncrementalWithKeyColumns("size == " + size, size, seed, false, new MutableInt(10),
                    TestJoinControl.DEFAULT_JOIN_CONTROL);
        }
    }
}
