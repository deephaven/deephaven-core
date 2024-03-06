//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.test.types.OutOfBandTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category(OutOfBandTest.class)
public class QueryTableWhereSingleTest extends QueryTableWhereTest {
    boolean oldParallel = QueryTable.FORCE_PARALLEL_WHERE;
    boolean oldDisable = QueryTable.DISABLE_PARALLEL_WHERE;

    @Before
    public void setUp() throws Exception {
        QueryTable.FORCE_PARALLEL_WHERE = false;
        QueryTable.DISABLE_PARALLEL_WHERE = true;
    }

    @After
    public void tearDown() throws Exception {
        QueryTable.FORCE_PARALLEL_WHERE = oldParallel;
        QueryTable.DISABLE_PARALLEL_WHERE = oldDisable;
    }
}
